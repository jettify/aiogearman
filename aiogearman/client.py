import asyncio

from .errors import GearmanWorkFailException, GearmanWorkException
from .log import logger
from .connection import create_connection

from .consts import SUBMIT_JOB, NULL, SUBMIT_JOB_LOW, SUBMIT_JOB_HIGH, \
    SUBMIT_JOB_LOW_BG, SUBMIT_JOB_HIGH_BG, SUBMIT_JOB_BG, WORK_COMPLETE, \
    WORK_DATA, WORK_EXCEPTION, WORK_FAIL

from .utils import unpack_first_arg


__all__ = ['create_client', 'GearmanClient']


def create_client(host='localhost', port=4730, loop=None):
    loop = loop or asyncio.get_event_loop()
    conn = yield from create_connection(host=host, port=port, loop=loop)
    return GearmanClient(conn)


class GearmanClient:
    """XXX"""

    def __init__(self, conn):
        self._conn = conn
        self._conn.register_push_cb(self._push)
        self._loop = self._conn.loop
        self._jobs = {}

    @asyncio.coroutine
    def _submit(self, submit_packet, function, data, unique_id=None):
        """Submit a job with the given function name and data."""
        unique_id = unique_id or b''
        packet_type, job_id = yield from self._conn.execute(
            submit_packet, function, unique_id, data)
        handle = JobHandle(function, data, unique_id, job_id, self._loop)
        self._jobs[job_id] = handle
        return handle

    @asyncio.coroutine
    def _submit_bg(self, submit_packet, function, data, unique_id=None):
        """Submit a job with the given function name and data."""
        unique_id = unique_id or b''

        packet_type, job_id = yield from self._conn.execute(
            submit_packet, function, data, unique_id)
        return job_id

    def _push(self, packet_type, data):
        job_id, result = unpack_first_arg(data)
        if packet_type == WORK_COMPLETE:
            job = self._jobs.pop(job_id)
            job._add_result(result)
            job._notify_complete()

        elif packet_type == WORK_DATA:
            job = self._jobs[job_id]
            job._add_result(result)

        elif packet_type == WORK_FAIL:
            job = self._jobs.pop(job_id)
            job._set_exception(GearmanWorkFailException())

        elif packet_type == WORK_EXCEPTION:
            job = self._jobs.pop(job_id)
            job._set_exception(GearmanWorkException(result))
        else:
            logger.warning("got packet_type: {}, how to handle?".format(
                packet_type))

    def submit(self, function, data, unique_id=None):
        """XXX

        :param function:
        :param data:
        :param unique_id:
        :return:
        """
        r = self._submit(SUBMIT_JOB, function, data, unique_id)
        return r

    def submit_high(self, function, data, unique_id=None):
        r = self._submit(SUBMIT_JOB_HIGH, function, data, unique_id)
        return r

    def submit_low(self, function, data, unique_id=None):
        r = self._submit(SUBMIT_JOB_LOW, function, data, unique_id)
        return r

    def submit_bg(self, function, data, unique_id=None):
        r = self._submit_bg(SUBMIT_JOB_BG, function, data, unique_id)
        return r

    def submit_high_bg(self, function, data, unique_id=None):
        r = self._submit_bg(self, SUBMIT_JOB_HIGH_BG, function, data,
                            unique_id)
        return r

    def submit_low_bg(self, function, data, unique_id=None):
        r = self._submit_bg(SUBMIT_JOB_LOW_BG, function, data, unique_id)
        return r

    def __repr__(self):
        return '<GearmanClient {}:{}>'.format(self._conn.host, self._conn.port)


class JobHandle:
    """XXX"""

    def __init__(self, function, data, unique_id, job_id, loop):
        self._function = function
        self._data = data
        self._unique_id = unique_id

        self._job_id = job_id

        self._work_data = None
        self._work_warnings = []

        self._loop = loop
        self._fut = asyncio.Future(loop=self._loop)

    def _add_result(self, data):
        if not self._work_data:
            self._work_data = data
        elif isinstance(self._work_data, list):
            self._work_data.append(data)
        else:
            self._work_data = [self._work_data]
            self._work_data.append(data)

    def _notify_complete(self):
        self._fut.set_result(self._work_data)

    def _set_exception(self, exc):
        self._fut.set_exception(exc)

    def wait_result(self):
        return self._fut

    @property
    def function(self):
        return self._function

    @property
    def data(self):
        return self._data

    @property
    def unique_id(self):
        return self._unique_id

    @property
    def job_id(self):
        return self._job_id

    @property
    def result(self):
        if not self._fut.done():
            raise Exception('Wait for job completion with yf')
        return self._work_data

    def __repr__(self):
        return '<JobHandle {}:{}>'.format(self._job_id, self._function)
