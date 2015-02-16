import asyncio
import struct

from collections import deque
from .utils import encode_command
from .log import logger
from .consts import REQ, WORK_COMPLETE, WORK_FAIL, NOOP, WORK_DATA, \
    WORK_WARNING, WORK_EXCEPTION


__all__ = ['create_connection', 'GearmanConnection']


@asyncio.coroutine
def create_connection(host='localhost', port=4730, loop=None):
    """XXX"""
    conn = GearmanConnection(host, port, loop=loop)
    yield from conn.connect()
    return conn


class GearmanConnection:
    """XXX"""

    PKT_FMT = struct.Struct(b">III")
    HEADER_LEN = struct.calcsize(b">III")

    _unsolicited = [WORK_COMPLETE, WORK_FAIL, NOOP,
                    WORK_DATA, WORK_WARNING, WORK_EXCEPTION]

    def __init__(self, host='localhost', port=4730, loop=None):

        self._host = host
        self._port = port

        self._reader = None
        self._writer = None
        self._reader_task = None

        self._loop = loop or asyncio.get_event_loop()
        self._requests = deque()
        self._closed = False
        self._push_callback = None

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def loop(self):
        return self._loop

    @property
    def closed(self):
        return self._closed

    def connect(self):
        self._reader, self._writer = yield from asyncio.open_connection(
            self._host, self._port, loop=self._loop)
        self._reader_task = asyncio.Task(self._read_data(), loop=self._loop)

    def execute(self, packet_type, *args, no_ack=False):
        """XXX

        :param packet_type:
        :param args:
        :param no_ack:
        :return:
        """
        assert self._reader and not self._reader.at_eof(), (
            "Connection closed or corrupted")
        command_raw = encode_command(REQ, packet_type, *args)
        self._writer.write(command_raw)
        fut = asyncio.Future(loop=self._loop)
        if no_ack:
            fut.set_result(None)
            return fut
        self._requests.append(fut)
        return fut

    @asyncio.coroutine
    def _read_data(self):
        """Response reader task."""
        try:
            while True:
                resp = yield from self._reader.readexactly(self.HEADER_LEN)
                magic, packet_type, size = self.PKT_FMT.unpack(resp)
                data = yield from self._reader.readexactly(size)
                if packet_type in self._unsolicited:
                    if self._push_callback is not None:
                        self._push_callback(packet_type, data)
                    continue
                fut = self._requests.pop()
                if not fut.cancelled():
                    fut.set_result((packet_type, data))

        except OSError as exc:
            conn_exc = ConnectionError("Gearman {0}:{1} went away".format(
                self._host, self._port))
            conn_exc.__cause__ = exc
            conn_exc.__context__ = exc
            fut = self._requests.pop()
            fut.set_exception(conn_exc)
            self.close()

    def _handle_unsolicited(self, packet_type, data):
        if self._push_callback is not None:
            self._push_callback(packet_type, data)

    def close(self):
        self._closed = True
        if self._reader:
            self._writer.close()
            self._reader = self._writer = None
            self._reader_task.cancel()
            for fut in self._requests:
                fut.cancel()
            self._requests = []

    def register_push_cb(self, callback):
        assert callable(callback)
        self._push_callback = callback

    def __repr__(self):
        return '<GearmanConnection {}:{}>'.format(self._host, self._port)
