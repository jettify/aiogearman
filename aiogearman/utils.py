import struct
from . import consts


_converters = {
    bytes: lambda val: val,
    bytearray: lambda val: val,
    str: lambda val: val.encode('utf-8'),
    int: lambda val: str(val).encode('utf-8'),
    float: lambda val: str(val).encode('utf-8'),
}


def encode_command(magic, packet_type, *args):
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
    header = struct.pack(consts.PKT_FMT, magic, packet_type, len(barg))
    return header + barg
