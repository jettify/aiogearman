import asyncio
from ._testutil import BaseTest, run_until_complete
from aiogearman import create_worker, GearmanWorkFailException
import aiogearman
from aiogearman.worker import Job


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

        class RevJob(Job):

            @asyncio.coroutine
            def function(self, data):
                return data[::-1]

        yield from worker.register_function(b'rev', RevJob)
        yield from worker.do_job()

        r = yield from job.wait_result()
        self.assertEqual(r, b'oof')

    @run_until_complete
    def test_do_jobs(self):
        client = yield from aiogearman.create_client(loop=self.loop)
        job1 = yield from client.submit(b'rev', b'foo')
        job2 = yield from client.submit(b'rev', b'bar')
        worker = yield from create_worker(loop=self.loop)

        class RevJob(Job):

            @asyncio.coroutine
            def function(self, data):
                return data[::-1]

        yield from worker.register_function(b'rev', RevJob)

        # helper function to process only two jobs
        x = 0

        def keep_going():
            nonlocal x
            x += 1
            return True if x < 3 else False

        yield from worker.do_jobs(keep_going=keep_going)

        jobs = [job1.wait_result(), job2.wait_result()]
        yield from asyncio.gather(*jobs, loop=self.loop)
        self.assertEqual(job1.result, b'oof')
        self.assertEqual(job2.result, b'rab')

    @run_until_complete
    def test_do_job_exception(self):
        client = yield from aiogearman.create_client(loop=self.loop)
        job = yield from client.submit(b'rev', b'foo')
        worker = yield from create_worker(loop=self.loop)

        class RevJob(Job):

            @asyncio.coroutine
            def function(self, data):
                raise Exception('log story short: error')

        yield from worker.register_function(b'rev', RevJob)
        yield from worker.do_job()

        with self.assertRaises(GearmanWorkFailException):
            yield from job.wait_result()
