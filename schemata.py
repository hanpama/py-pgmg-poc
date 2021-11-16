import common
import typing


class Schemata(typing.NamedTuple):
    catalog_name: str
    schema_name: str
    schema_owner: str
    default_character_set_catalog: str
    default_character_set_schema: str
    default_character_set_name: str
    sql_path: str

    @classmethod
    def build(
        cls,
        catalog_name: typing.Optional[str] = None,
        schema_name: typing.Optional[str] = None,
        schema_owner: typing.Optional[str] = None,
        default_character_set_catalog: typing.Optional[str] = None,
        default_character_set_schema: typing.Optional[str] = None,
        default_character_set_name: typing.Optional[str] = None,
        sql_path: typing.Optional[str] = None,
    ):
        return cls(
            typing.cast(str, catalog_name),
            typing.cast(str, schema_name),
            typing.cast(str, schema_owner),
            typing.cast(str, default_character_set_catalog),
            typing.cast(str, default_character_set_schema),
            typing.cast(str, default_character_set_name),
            typing.cast(str, sql_path),
        )

    class Alias(common.TableReference):
        def __init__(self, alias: typing.Optional[str] = None):
            super().__init__("information_schema", "schemata", alias)
            self.catalog_name = common.ColumnReference(self, "catalog_name")
            self.schema_name = common.ColumnReference(self, "schema_name")
            self.schema_owner = common.ColumnReference(self, "schema_owner")
            self.default_character_set_catalog = common.ColumnReference(
                self, "default_character_set_catalog")
            self.default_character_set_schema = common.ColumnReference(
                self, "default_character_set_schema")
            self.default_character_set_name = common.ColumnReference(
                self, "default_character_set_name")
            self.sql_path = common.ColumnReference(self, "sql_path")

    Table = common.TableGateway["Schemata", typing.Union[
        typing.Literal["catalog_name"],
        typing.Literal["schema_name"],
        typing.Literal["schema_owner"],
        typing.Literal["default_character_set_catalog"],
        typing.Literal["default_character_set_schema"],
        typing.Literal["default_character_set_name"],
        typing.Literal["sql_path"],
    ]](
        lambda values: Schemata(*values),
        common.TableMetadata(
            table_schema="information_schema",
            table_name="schemata",
            column_fields=(
                common.ColumnMetadata(
                    "catalog_name", "catalog_name", "name", False),
                common.ColumnMetadata(
                    "schema_name", "schema_name", "name", False),
                common.ColumnMetadata(
                    "schema_owner", "schema_owner", "name", False),
                common.ColumnMetadata(
                    "default_character_set_catalog", "default_character_set_catalog", "name", False),
                common.ColumnMetadata(
                    "default_character_set_schema", "default_character_set_schema", "name", False),
                common.ColumnMetadata(
                    "default_character_set_name", "default_character_set_name", "name", False),
                common.ColumnMetadata("sql_path", "sql_path",
                                      "character varying", False),
            ),
        ),
    )
