import asyncio
from ._testutil import BaseTest, run_until_complete
from aiogearman import create_worker, GearmanWorkException
import aiogearman


class WorkerTest(BaseTest):

    @run_until_complete
    def test_worker_ctor(self):
        worker = yield from create_worker(loop=self.loop)
        yield from worker.set_worker_id(42)
        resp = yield from worker.echo(b'foo')
        self.assertEqual(resp, b'foo')
        worker.close()

    @run_until_complete
    def test_do_job(self):
        client = yield from aiogearman.create_client(loop=self.loop)
        job = yield from client.submit(b'rev', b'foo')
        worker = yield from create_worker(loop=self.loop)

        @asyncio.coroutine
        def rev(data):
            return data[::-1]
        yield from worker.register_function(b'rev', rev)
        yield from worker.do_job()

        r = yield from job.wait_result()
        self.assertEqual(r, b'oof')

    @run_until_complete
    def test_do_job_exception(self):
        client = yield from aiogearman.create_client(loop=self.loop)
        job = yield from client.submit(b'rev', b'foo')
        worker = yield from create_worker(loop=self.loop)

        @asyncio.coroutine
        def rev(data):
            raise Exception('log story short: error')

        yield from worker.register_function(b'rev', rev)
        yield from worker.do_job()
        with self.assertRaises(GearmanWorkException):
            yield from job.wait_result()
