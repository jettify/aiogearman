import asyncio
from unittest import mock
from ._testutil import BaseTest, run_until_complete

from aiogearman.consts import ECHO_REQ, ECHO_RES, REQ
from aiogearman.connection import create_connection, GearmanConnection
from aiogearman.utils import encode_command


class ConnectionTest(BaseTest):

    @run_until_complete
    def test_connect(self):
        conn = yield from create_connection(loop=self.loop)
        self.assertTrue(conn._loop, self.loop)
        packet_type, data = yield from conn.execute(ECHO_REQ, b'foo')
        self.assertEqual(packet_type, ECHO_RES)
        self.assertEqual(data, b'foo')
        conn.close()

    @run_until_complete
    def test_connection_ctor_global_loop(self):
        asyncio.set_event_loop(self.loop)
        conn = yield from create_connection()
        self.assertEqual(conn.loop, self.loop)
        self.assertEqual(conn.host, 'localhost')
        self.assertEqual(conn.port, 4730)
        self.assertEqual(conn.closed, False)
        self.assertTrue('Gearman' in conn.__repr__())
        conn.close()
        conn.close()
        self.assertEqual(conn.closed, True)

    @run_until_complete
    def test_connect_close(self):
        conn = yield from create_connection(loop=self.loop)
        self.assertTrue(conn._loop, self.loop)
        resp_fut1 = conn.execute(ECHO_REQ, b'foo')
        resp_fut2 = conn.execute(ECHO_REQ, b'foo')
        conn.close()
        self.assertTrue(resp_fut1.cancelled)
        self.assertTrue(resp_fut2.cancelled)

    @run_until_complete
    def test_execute_cancel_future(self):
        conn = yield from create_connection(loop=self.loop)
        self.assertTrue(conn._loop, self.loop)
        resp_fut1 = conn.execute(ECHO_REQ, b'foo')
        resp_fut1.cancel()

        packet_type, data = yield from conn.execute(ECHO_REQ, b'foo')
        conn.close()
        self.assertEqual(packet_type, ECHO_RES)
        self.assertEqual(data, b'foo')

    def test_encode_command(self):
        res = encode_command(REQ, ECHO_RES, 'foo', 3.14)
        expected = b'\x00REQ\x00\x00\x00\x11\x00\x00\x00\x08foo\x003.14'
        self.assertEqual(res, expected)
        res = encode_command(REQ, ECHO_RES, b'foo', bytearray(b'Q'))
        expected = b'\x00REQ\x00\x00\x00\x11\x00\x00\x00\x05foo\x00Q'
        self.assertEqual(res, expected)

        with self.assertRaises(TypeError):
            encode_command(REQ, ECHO_RES, object())

    @run_until_complete
    def test_osserror(self):
        conn = GearmanConnection(loop=self.loop)

        @asyncio.coroutine
        def invoke_osserror(*a, **kw):
            yield from asyncio.sleep(0.1, loop=self.loop)
            raise OSError

        # setup reader
        reader = mock.MagicMock()
        reader.readexactly.return_value = invoke_osserror()
        reader.at_eof.return_value = False
        writer = mock.MagicMock()
        conn._reader = reader
        conn._writer = writer
        conn._read_task = asyncio.async(conn._read_data(), loop=self.loop)

        with self.assertRaises(ConnectionError):
            yield from conn.execute(ECHO_RES, 'foo')
