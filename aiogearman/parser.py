import struct
from . import consts


class Reader:

    def __init__(self):
        self._buffer = bytearray()

        self._is_header = False
        self._payload_size = None

    def feed(self, chunk):
        """Put raw chunk of data obtained from connection to buffer.
        :param data: ``bytes``, raw input data.
        """
        if not chunk:
            return
        self._buffer.extend(chunk)

    def gets(self):
        """

        :return:
        """
        buffer_size = len(self._buffer)
        if not self._is_header and buffer_size >= consts.HEADER_LEN:
            magic, cmd, size = struct.unpack(consts.PKT_FMT,
                                 self._buffer[:consts.HEADER_LEN])
            self._payload_size = size
            self._is_header = True

        if (self._is_header and
                    buffer_size >= consts.HEADER_LEN + self._payload_size):
            start = consts.HEADER_LEN
            end = consts.HEADER_LEN + self._payload_size

            raw_resp = self._buffer[start, end]
            resp = bytes(raw_resp).split(consts.NULL)
            self._reset()
            return resp
        return False

    def _reset(self):
        start = consts.HEADER_LEN + self._payload_size
        self._buffer = self._buffer[start:]
        self._is_header = False
        self._payload_size = None


_converters = {
    bytes: lambda val: val,
    bytearray: lambda val: val,
    str: lambda val: val.encode('utf-8'),
    int: lambda val: str(val).encode('utf-8'),
    float: lambda val: str(val).encode('utf-8'),
}


def encode_command(magic, packet, *args):
    """XXX"""
    buf = bytearray()
    add = lambda data: buf.extend(data + b'\00')

    for arg in args:
        if type(arg) in _converters:
            barg = _converters[type(arg)](arg)
            add(barg)
        else:
            raise TypeError("Argument {!r} expected to be of bytes,"
                            " str, int or float type".format(arg))
    header = struct.pack(consts.PKT_FMT, magic, packet, len(barg))
    return header + barg
