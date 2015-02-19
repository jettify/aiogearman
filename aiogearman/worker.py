import asyncio
import sys
from .log import logger
from .connection import create_connection
from .consts import SET_CLIENT_ID, CAN_DO, \
    GRAB_JOB, WORK_COMPLETE, WORK_EXCEPTION, WORK_FAIL, ECHO_REQ, PRE_SLEEP, \
    NO_JOB, NOOP


__all__ = ['create_worker', 'GearmanWorker']


def create_worker(host='localhost', port=4730, loop=None):
    """XXX

    :param host:
    :param port:
    :param loop:
    :return:
    """
    loop = loop or asyncio.get_event_loop()
    conn = yield from create_connection(host=host, port=port, loop=loop)
    return GearmanWorker(conn)


class GearmanWorker:

    def __init__(self, conn):
        self._conn = conn
        # assert conn.closed, 'Attempt to use worker with closed connection'

        self._conn.register_push_cb(self._push)
        self._loop = self._conn.loop

        self._functions = {}
        self._sleep_waiter = None

    @asyncio.coroutine
    def set_worker_id(self, worker_id):
        yield from self._conn.execute(SET_CLIENT_ID, worker_id, no_ack=True)

    @asyncio.coroutine
    def register_function(self, name, function):
        self._functions[name] = function
        yield from self._conn.execute(CAN_DO, name, no_ack=True)

    @asyncio.coroutine
    def get_job(self):
        if self._sleep_waiter:
            yield from self._sleep()

        packet_type, rest = yield from self._conn.execute(GRAB_JOB)

        while packet_type == NO_JOB:
            yield from self._sleep()
            packet_type, rest = yield from self._conn.execute(GRAB_JOB)
        job_id, func, data = rest.split(b'\0', 2)
        return job_id, func, data

    @asyncio.coroutine
    def _sleep(self):
        if not self._sleep_waiter:
            self._sleep_waiter = asyncio.Future(loop=self._loop)
            self._conn.execute(PRE_SLEEP, no_ack=True)
        return self._sleep_waiter

    @asyncio.coroutine
    def _work(self, job_id, function, data):
        func = self._functions[function]
        try:
            result = yield from func(data)
            yield from self._conn.execute(WORK_COMPLETE, job_id, result,
                                          no_ack=True)
        except Exception:
            etype, emsg, bt = sys.exc_info()
            logger.error("Worker error: {}:{}".format(etype, emsg))
            msg = b'%s(%s)' % (etype.__name__, emsg)
            yield from self._conn.execute(WORK_EXCEPTION, job_id, msg,
                                          no_ack=True)
            yield from self._conn.execute(WORK_FAIL, job_id,
                                          no_ack=True)

    @asyncio.coroutine
    def do_job(self):
        job_id, func, data = yield from self.get_job()
        yield from self._work(job_id, func, data)

    @asyncio.coroutine
    def do_jobs(self, keep_going=lambda: True):
        while keep_going:
            yield from self.do_job()

    def _push(self, packet_type, data):
        if packet_type == NOOP and self._sleep_waiter:
            fut, self._sleep_waiter = self._sleep_waiter, None
            fut.set_result(None)
            self._sleep_waiter = None
        else:
            logger.warning('Unexpected packet_type: {}'.format(packet_type))

    @asyncio.coroutine
    def echo(self, data):
        """XXX

        :param data:
        :return:
        """
        packet_type, resp = yield from self._conn.execute(
            ECHO_REQ, data)
        return resp

    def close(self):
        self._conn.close()

    def __repr__(self):
        return '<GearmanWorker {}:{}>'.format(self._conn.host, self._conn.port)
