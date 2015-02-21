import asyncio
import unittest

from functools import wraps
from aiogearman import create_worker


def run_until_complete(fun):
    if not asyncio.iscoroutinefunction(fun):
        fun = asyncio.coroutine(fun)

    @wraps(fun)
    def wrapper(test, *args, **kw):
        loop = test.loop
        ret = loop.run_until_complete(fun(test, *args, **kw))
        return ret
    return wrapper


class BaseTest(unittest.TestCase):
    """Base test case for unittests.
    """

    def setUp(self):
        asyncio.set_event_loop(None)
        self.loop = asyncio.new_event_loop()

    def tearDown(self):
        self.loop.close()
        del self.loop


class GearmanTest(BaseTest):

    def setUp(self):
        super().setUp()
        self.worker = self.loop.run_until_complete(self._create_worker())

    def tearDown(self):
        self.worker.close()
        super().tearDown()

    @asyncio.coroutine
    def _create_worker(self):
        worker = yield from create_worker(loop=self.loop)

        @asyncio.coroutine
        def rev(data):
            return data[::-1]
        yield from worker.register_function(b'rev', rev)
        return worker
