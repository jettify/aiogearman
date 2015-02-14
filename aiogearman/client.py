import asyncio
from aiogearman import GearmanWorkFailException, GearmanWorkException
from aiogearman.log import logger
from .connection import create_connection

from .consts import SUBMIT_JOB, NULL, SUBMIT_JOB_LOW, SUBMIT_JOB_HIGH, \
    SUBMIT_JOB_LOW_BG, SUBMIT_JOB_HIGH_BG, SUBMIT_JOB_BG, WORK_COMPLETE, \
    WORK_DATA, WORK_EXCEPTION, WORK_FAIL

from .utils import JobHandle, unpack_first_arg


def create_client(host='localhost', port=4730, loop=None):
    loop = loop or asyncio.get_event_loop()
    conn = yield from create_connection(host=host, port=port, loop=loop)
    return GearmanClient(conn)


class GearmanClient:
    """

    """

    def __init__(self, conn):
        self._conn = conn
        self._conn.register_push_cb(self._push)
        self._loop = self._conn.loop
        self._jobs = {}

    @asyncio.coroutine
    def _submit(self, submit_packet, function, data, unique_id=NULL):
        """Submit a job with the given function name and data."""
        packet_type, job_id = yield from self._conn.execute(
            submit_packet, function, data, unique_id)
        handle = JobHandle(function, data, unique_id, job_id, self._loop)
        self._jobs[job_id] = handle
        return handle

    @asyncio.coroutine
    def _submit_bg(self, submit_packet, function, data, unique_id=NULL):
        """Submit a job with the given function name and data."""
        packet_type, job_id = yield from self._conn.execute(
            submit_packet, function, data, unique_id)
        return job_id

    def submit(self, function, data, unique_id=NULL):
        r = self._submit(SUBMIT_JOB, function, data, unique_id)
        return r

    def submit_high(self, function, data, unique_id=NULL):
        r = self._submit(SUBMIT_JOB_HIGH, function, data, unique_id)
        return r

    def submit_low(self, function, data, unique_id=NULL):
        r = self._submit(SUBMIT_JOB_LOW, function, data, unique_id)
        return r

    def submit_bg(self, function, data, unique_id=NULL):
        r = self._submit_bg(SUBMIT_JOB_BG, function, data, unique_id)
        return r

    def submit_high_bg(self, function, data, unique_id=NULL):
        r = self._submit_bg(self, SUBMIT_JOB_HIGH_BG, function, data,
                            unique_id)
        return r

    def submit_low_bg(self, function, data, unique_id=NULL):
        r = self._submit_bg(SUBMIT_JOB_LOW_BG, function, data, unique_id)
        return r

    def _push(self, packet_type, data):
        if packet_type == WORK_COMPLETE:
            job_id, result = unpack_first_arg(data)
            job = self._jobs.pop(job_id)
            job._add_result(result)
            job._notify()

        elif packet_type == WORK_DATA:
            job_id, result = unpack_first_arg(data)
            job = self._jobs[job_id]
            job._add_result(result)

        elif packet_type == WORK_FAIL:
            job_id, result = unpack_first_arg(data)
            job = self._jobs.pop(job_id)
            job._set_exception(GearmanWorkFailException())

        elif packet_type == WORK_EXCEPTION:
            job_id, result = unpack_first_arg(data)
            job = self._jobs.pop(job_id)
            job._set_exception(GearmanWorkException(result))
        else:
            logger.warning("got packet_type: {}, how to handle?".format(
                packet_type))


