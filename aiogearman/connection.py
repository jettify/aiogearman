import asyncio
import struct

from collections import deque
from .utils import encode_command
from .log import logger


@asyncio.coroutine
def create_connection(host='localhost', port=4730, loop=None):
    """XXX"""
    conn = GearmanConnection(host, port, loop=loop)
    yield from conn._connect()
    return conn


class GearmanConnection:
    """XXX"""

    PKT_FMT = struct.Struct(b">III")
    HEADER_LEN = struct.calcsize(b">III")

    def __init__(self, host='localhost', port=4730, loop=None):

        self._host = host
        self._port = port

        self._reader = None
        self._writer = None
        self._reader_task = None

        self._loop = loop or asyncio.get_event_loop()
        self._requests = deque()
        self._closing = False
        self._closed = False

    def _connect(self):
        self._reader, self._writer = yield from asyncio.open_connection(
            self._host, self._port, loop=self._loop)
        self._reader_task = asyncio.Task(self._read_data(), loop=self._loop)

    def execute(self, magic, packet_type, *args):
        """XXX"""
        assert self._reader and not self._reader.at_eof(), (
            "Connection closed or corrupted")
        fut = asyncio.Future(loop=self._loop)
        command_raw = encode_command(magic, packet_type, *args)
        self._writer.write(command_raw)
        self._requests.append(fut)
        return fut

    def close(self):
        """Close connection."""
        self._do_close()

    def _do_close(self, exc=None):
        if exc:
            logger.error("Connection closed with error: {}".format(exc))
        if self._closed:
            return
        self._closed = True
        self._closing = False
        self._writer.transport.close()
        self._reader_task.cancel()

    @asyncio.coroutine
    def _read_data(self):
        """Response reader task."""
        try:
            while True:
                resp = yield from self._reader.readexactly(self.HEADER_LEN)
                magic, packet_type, size = self.PKT_FMT.unpack(resp)
                data = yield from self._reader.readexactly(size)

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

    def close(self):
        if self._reader:
            self._writer.close()
            self._reader = self._writer = None
            self._reader_task.cancel()
            for fut in self._requests:
                fut.cancel()
            self._requests = []



    def __repr__(self):
        return '<GearmanConnection>'
