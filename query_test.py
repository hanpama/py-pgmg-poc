import asyncio
import typing
import unittest

import asyncpg

from . import common
from .schemata import Schemata


class SQLRendererTest(unittest.TestCase):

    def test_get_by(self):
        asyncio.run(self._test_get_by())

    async def _test_get_by(self):
        sql_handle = TestSQLHandle()

        res = await Schemata.Table.get_by(
            sql_handle,
            ('schema_name',),
            ('public',),
            ('information_schema',),
        )
        print(res)

    def test_find_by(self):
        asyncio.run(self._test_find_by())

    async def _test_find_by(self):
        sql_handle = TestSQLHandle()

        res = await Schemata.Table.find_by(
            sql_handle,
            ('catalog_name',),
            ('postgres',),
        )
        print(res)


class TestSQLHandle(common.SQLHandle):
    def __init__(self):
        self.conn = asyncpg.connect('postgres://localhost:5432/postgres')
        self.tx_level = 0

    async def aquire_connection(self):
        conn: asyncpg.Connection = await self.conn
        return conn

    async def query(self, sql: str, *args: typing.Any):
        conn = await self.aquire_connection()
        return await conn.fetch(sql, *args)

    async def exec(self, sql: str, *args: typing.Any) -> None:
        conn = await self.aquire_connection()
        await conn.execute(sql, *args)

    async def transaction(self):
        conn = await self.aquire_connection()
        tx = self.Tx(conn, self.tx_level)
        self.tx_level += 1
        return tx

    class Tx:
        def __init__(self, conn: asyncpg.Connection, level: int):
            self.conn = conn
            self.level = level

        async def __aenter__(self):
            if self.level == 0:
                await self.conn.execute('BEGIN TRANSACTION;')
            else:
                await self.conn.execute(f'SAVEPOINT "{self.level}";')

        async def __aexit__(self, exc_type, exc_value, traceback):
            if isinstance(exc_value, Exception):
                if self.level == 0:
                    await self.conn.execute('ROLLBACK;')
                else:
                    await self.conn.execute(f'ROLLBACK TO SAVEPOINT "{self.level}')

            else:
                if self.level == 0:
                    await self.conn.execute('COMMIT;')
                else:
                    await self.conn.execute(f'RELEASE SAVEPOINT "{self.level}')
