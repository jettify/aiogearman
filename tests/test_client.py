from ._testutil import BaseTest, run_until_complete
from aiogearman import create_client


class ClientTest(BaseTest):

    @run_until_complete
    def test_client_ctor(self):
        client = yield from create_client(loop=self.loop)
        job = yield from client.submit('echo', 'test\na\n', unique_id=123)
        result = yield from job.wait_result()
        self.assertTrue(result)
