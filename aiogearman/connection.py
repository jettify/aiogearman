import asyncio


from collections import deque
from .parser import encode_command, Reader
from .log import logger


@asyncio.coroutine
def create_connection(host='localhost', port=4730, loop=None):
    """XXX"""
    reader, writer = yield from asyncio.open_connection(
        host, port, loop=loop)
    conn = GearmanConnection(reader, writer, loop=loop)
    return conn


class GearmanConnection:
    """XXX"""

    def __init__(self, reader, writer, *, loop=None):

        self._reader, self._writer = reader, writer
        self._loop = loop or asyncio.get_event_loop()
        self._parser = Reader()
        self._cmd_waiters = deque()
        self._closing = False
        self._closed = False
        self._reader_task = asyncio.Task(self._read_data(), loop=self._loop)

    def execute(self, magic, packet, *args):
        """XXX"""
        assert self._reader and not self._reader.at_eof(), (
            "Connection closed or corrupted")
        fut = asyncio.Future(loop=self._loop)
        command_raw = encode_command(magic, packet, *args)
        self._writer.write(command_raw)
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
        is_canceled = False
        while not self._reader.at_eof():
            try:
                data = yield from self._reader.read(consts.MAX_CHUNK_SIZE)
            except asyncio.CancelledError:
                is_canceled = True
                break
            except Exception as exc:
                break
            self._parser.feed(data)

        if is_canceled:
            # useful during update to TLS, task canceled but connection
            # should not be closed
            return
        self._closing = True
        self._loop.call_soon(self._do_close, None)

    def _parse_data(self):
        pass

    def __repr__(self):
        return '<GearmanConnection>'
