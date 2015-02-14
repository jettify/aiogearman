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
