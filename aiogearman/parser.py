import struct
from .consts import PKT_FMT


class Reader:

    def feed(self, chunk):
        """

        :return:
        """

    def gets(self):
        """

        :return:
        """



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
    header = struct.pack(PKT_FMT, magic, packet, len(barg))
    return header + barg
