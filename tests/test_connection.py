import asyncio
from ._testutil import BaseTest, run_until_complete
from aiogearman.connection import create_connection

class ConnectionTest(BaseTest):

    @run_until_complete
    def test_connect(self):

        import ipdb; ipdb.set_trace()
        conn = yield from create_connection(loop=self.loop)

        # res = yield from conn.execute(b'ping')
        # self.assertTrue(res)
        self.assertTrue(conn._loop, self.loop)
        conn.close()
