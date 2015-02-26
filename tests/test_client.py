from ._testutil import GearmanTest, run_until_complete
from aiogearman import create_client


class ClientTest(GearmanTest):

    @run_until_complete
    def test_client_ctor(self):
        client = yield from create_client(loop=self.loop)
        job = yield from client.submit(b'rev', b'submit', unique_id=1)

        self.assertEqual(job.function, b'rev')
        self.assertEqual(job.data, b'submit')
        self.assertEqual(job.unique_id, 1)
        self.assertTrue(job.job_id, 3)

        yield from self.worker.do_job()

        result = yield from job.wait_result()
        self.assertEqual(result, job.result)
        self.assertEqual(result, b'timbus')

        self.assertTrue('Gearman' in client.__repr__())

    @run_until_complete
    def test_client_submit_high(self):
        client = yield from create_client(loop=self.loop)
        job = yield from client.submit_high(b'rev', b'submit_high',
                                            unique_id=4)
        yield from self.worker.do_job()
        result = yield from job.wait_result()
        self.assertEqual(result, b'hgih_timbus')

    @run_until_complete
    def test_client_submit_low(self):
        client = yield from create_client(loop=self.loop)
        job = yield from client.submit_low(b'rev', b'submit_low',
                                           unique_id=5)
        yield from self.worker.do_job()
        result = yield from job.wait_result()
        self.assertEqual(result, b'wol_timbus')

    @run_until_complete
    def test_client_submit_bg(self):
        client = yield from create_client(loop=self.loop)
        job = yield from client.submit_bg(b'rev', b'submit_bg',
                                          unique_id=6)
        yield from self.worker.do_job()
        self.assertTrue(job)

    @run_until_complete
    def test_client_submit_high_bg(self):
        client = yield from create_client(loop=self.loop)
        job = yield from client.submit_high_bg(b'rev', b'submit_high_bg',
                                               unique_id=7)
        yield from self.worker.do_job()
        self.assertTrue(job)

    @run_until_complete
    def test_client_submit_low_bg(self):
        client = yield from create_client(loop=self.loop)
        job = yield from client.submit_low_bg(b'rev', b'submit_low',
                                              unique_id=8)
        yield from self.worker.do_job()
        self.assertTrue(job)
