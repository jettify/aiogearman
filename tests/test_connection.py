import asyncio
from ._testutil import BaseTest, run_until_complete
from aiogearman.consts import ECHO_REQ, ECHO_RES
from aiogearman.connection import create_connection


class ConnectionTest(BaseTest):

    @run_until_complete
    def test_connect(self):
        conn = yield from create_connection(loop=self.loop)
        self.assertTrue(conn._loop, self.loop)
        packet_type, data = yield from conn.execute(ECHO_REQ, b'foo')
        self.assertEqual(packet_type, ECHO_RES)
        self.assertEqual(data, b'foo')
        conn.close()

    @run_until_complete
    def test_connection_ctor_global_loop(self):
        asyncio.set_event_loop(self.loop)
        conn = yield from create_connection()
        self.assertEqual(conn.loop, self.loop)
        self.assertEqual(conn.host, 'localhost')
        self.assertEqual(conn.port, 4730)
        self.assertEqual(conn.closed, False)
        self.assertTrue('Gearman' in conn.__repr__())
        conn.close()
        conn.close()
        self.assertEqual(conn.closed, True)
