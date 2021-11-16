import typing
from copy import copy


class SQLHandle(typing.Protocol):
    """
    SQLHandle
    """
    async def query(
        self, sql: str, * args: typing.Any) -> list[tuple]: ...

    async def exec(
        self, sql: str, *args: typing.Any) -> int: ...


class ColumnMetadata:
    def __init__(
        self,
        column_name: str,
        field_name: str,
        sql_data_type: str,
        is_array: bool,
    ):
        self.column_name = column_name
        self.field_name = field_name
        self.sql_data_type = sql_data_type
        self.is_array = is_array


class TableMetadata:
    def __init__(
        self,
        table_schema: str,
        table_name: str,
        column_fields: tuple[ColumnMetadata, ...],
        primary_key: typing.Optional[set[str]] = None,
    ):
        self.table_schema = table_schema
        self.table_name = table_name
        self.columns = column_fields
        self.primary_key = primary_key


class TableReference:
    def __init__(
            self,
            table_schema: str,
            table_name: str,
            table_alias: typing.Optional[str] = None,
    ):
        self.table_schema = table_schema
        self.table_name = table_name
        self.table_alias = table_alias
        self.columns = []

    columns: list['ColumnReference']

    def alias(self, table_alias):
        alias_ref = copy(self)
        alias_ref.alias = table_alias
        return alias_ref

    def __str__(self):
        schema_qualified_name = f'{_q(self.table_schema)}.{_q(self.table_name)}'
        if self.table_alias:
            return f'{schema_qualified_name} AS {_q(self.table_alias)}'
        else:
            return schema_qualified_name


class ColumnReference:
    def __init__(
        self,
        table_reference: TableReference,
        column_name: str,
    ):
        self.table_reference = table_reference
        self.column_name = column_name

    def __str__(self):
        return f'{self.table_reference}.{_q(self.column_name)}'


TRecord = typing.TypeVar('TRecord', bound=typing.NamedTuple)
TField = typing.TypeVar('TField', bound=str)


class TableGateway(typing.Generic[TRecord, TField]):
    """
    TableGateway provides row access methods.
    """

    def __init__(
        self,
        constructor: typing.Callable[[tuple], TRecord],
        meta: TableMetadata,
        newline: str = '\n',
    ):
        self.constructor = constructor
        self.meta = meta
        self.newline = newline

    async def get_by(self, sql_handle: SQLHandle, by: typing.Sequence[TField], *keys: tuple):
        """
        Each keys are expected to have one or zero row matched.
        If multiple rows are matched to the condition,
        the result set is unpredictable.
        """
        _n = self.newline

        table = self.meta
        columns = self.meta.columns
        matches = {c: i for i, c in enumerate(columns) if c.field_name in by}

        kcss = _render_selection_set(matches)
        tacss = _render_aliased_selection_set('__t', columns)
        kacss = _render_aliased_selection_set('__k', matches)

        sql = _n.join((
            f'WITH __k AS ({_render_unnested_selection(matches)})',
            f'SELECT DISTINCT ON ({kacss}) {tacss}',
            f'FROM __k JOIN {_q(table.table_schema)}.{_q(table.table_name)} AS __t USING ({kcss})',
        ))
        arguments = (list(arg_sec) for arg_sec in zip(*keys))

        rows = await sql_handle.query(sql, *arguments)

        key_objs: typing.Mapping[tuple, TRecord] = {}
        for row in rows:
            key = tuple(row[i] for i in matches.values())
            obj = self.constructor(row)
            key_objs[key] = obj

        return [key_objs.get(key) for key in keys]

    async def find_by(self, sql_handle: SQLHandle, by: typing.Sequence[TField], *keys: tuple):
        """
        find_by returns a list of rows, each corresponding to the given key.
        """
        _n = self.newline

        table = self.meta
        columns = self.meta.columns
        matches = {c: i for i, c in enumerate(columns) if c.field_name in by}

        kcss = _render_selection_set(matches.keys())
        tacss = _render_aliased_selection_set('__t', columns)

        sql = _n.join((
            f'WITH __k AS ({_render_unnested_selection(matches)})',
            f'SELECT {tacss}',
            f'FROM __k LEFT JOIN {_q(table.table_schema)}.{_q(table.table_name)} AS __t USING ({kcss})',
        ))
        arguments = (list(arg_sec) for arg_sec in zip(*keys))

        rows = await sql_handle.query(sql, *arguments)

        key_objs: typing.Mapping[tuple, list[TRecord]] = {}
        for row in rows:
            key = tuple(row[i] for i in matches.values())
            obj = self.constructor(row)

            objs = key_objs.get(key)
            if objs is None:
                objs = []
                key_objs[key] = objs

            objs.append(obj)

        return [key_objs.get(key, []) for key in keys]

    async def insert(self, sql_handle: SQLHandle, *records: TRecord):
        """
        insert given records into table.
        """
        _n = self.newline

        table = self.meta
        columns = self.meta.columns

        sql = _n.join((
            f'WITH __v AS ({_render_unnested_selection(columns)})',
            f'INSERT INTO {_q(table.table_schema)}.{_q(table.table_name)}',
            f'SELECT * FROM __v',
        ))
        arguments = (list(arg_sec) for arg_sec in zip(*records))

        await sql_handle.exec(sql, *arguments)

    async def save(self, sql_handle: SQLHandle, *records: TRecord):
        """
        save given records to table. It raises an exception when the table has
        no primary pk.
        """
        _n = self.newline

        table = self.meta
        columns = self.meta.columns
        pk = self.meta.primary_key
        if not pk:
            raise Exception('Table {}.{} has no primary key'.format(
                table.table_schema,
                table.table_name,
            ))
        matches = {c for c in columns if c.field_name in pk}
        setters = {c for c in columns if c.field_name not in pk}

        kcss = _render_selection_set(matches)
        tcss = _render_selection_set(columns)
        vacss = _render_aliased_selection_set('__v', columns)

        sql = _n.join((
            f'WITH __v AS ({_render_unnested_selection(columns)})',
            f'INSERT INTO {_q(table.table_schema)}.{_q(table.table_name)} AS __t',
            f'SELECT * FROM __v',
            f'ON CONFILICT ({kcss}) DO UPDATE ({tcss}) = ({vacss})'
            f'WHERE {_render_tk_match("__t", "__v", setters)}'
        ))
        arguments = (list(arg_sec) for arg_sec in zip(*records))

        await sql_handle.exec(sql, *arguments)

    async def delete(self, sql_handle: SQLHandle, *records: TRecord):
        """
        delete given records from table. It raises an exception when the table has
        no primary pk.
        """
        _n = self.newline

        table = self.meta
        columns = self.meta.columns
        pk = self.meta.primary_key
        if not pk:
            raise Exception('Table {}.{} has no primary key'.format(
                table.table_schema,
                table.table_name,
            ))
        matches = {c: i for i, c in enumerate(columns) if c.field_name in pk}

        sql = _n.join((
            f'WITH __k AS ({_render_unnested_selection(matches)})',
            f'DELETE FROM {_q(table.table_schema)}.{_q(table.table_name)} AS __t',
            f'USING __k WHERE {_render_tk_match("__t", "__k", matches)}',
        ))
        arguments = (
            [getattr(rec, col.column_name) for rec in records]
            for col in matches
        )
        await sql_handle.exec(sql, *arguments)


def _render_unnested_selection(cols: typing.Iterable[ColumnMetadata], ):
    return f'SELECT DISTINCT ON (__k.*) __k.* AS __n FROM UNNEST(' \
        + ', '.join(
            f'${i+1}::{col.sql_data_type}[]' for i, col in enumerate(cols)
        ) \
        + f') AS __k({_render_selection_set(cols)})'


def _render_selection_set(cols: typing.Iterable[ColumnMetadata]):
    return _render_list(_q(col.column_name) for col in cols)


def _render_aliased_selection_set(table_alias: str, cols: typing.Iterable[ColumnMetadata]):
    return _render_list(_column_alias(table_alias, col.column_name) for col in cols)


def _q(name: str):
    return '"' + name.replace('"', '""') + '"'


def _render_list(tokens: typing.Iterable[str]):
    return ', '.join(tokens)


def _column_alias(table_alias: str, col: str):
    return _q(table_alias) + '.' + _q(col)


def _render_tk_match(t: str, k: str, columns: typing.Iterable[ColumnMetadata]):
    return ' AND '.join(
        f'{_q(t)}.{_q(c.column_name)} = {_q(k)}.{_q(c.column_name)}' for c in columns
    )
