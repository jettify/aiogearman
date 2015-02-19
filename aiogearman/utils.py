import struct
from .consts import REQ, RES


_converters = {
    bytes: lambda val: val,
    bytearray: lambda val: val,
    str: lambda val: val.encode('utf-8'),
    int: lambda val: str(val).encode('utf-8'),
    float: lambda val: str(val).encode('utf-8'),
}


def encode_command(magic, packet_type, *args):
    """XXX

    :param magic:
    :param packet_type:
    :param args:
    :return:
    """
    assert magic in [REQ, RES], 'magic number must be REQ or RES value'
    _args = []
    for arg in args:
        if type(arg) in _converters:
            barg = _converters[type(arg)](arg)
            _args.append(barg)
        else:
            raise TypeError("Argument {!r} expected to be of bytes,"
                            " str, int or float type".format(arg))
    buf = b'\0'.join(_args)
    header = magic + struct.pack(">II", packet_type, len(buf))
    return header + buf


def unpack_first_arg(data):
    """XXX

    :param data:
    :return:
    """
    pos = data.find(b'\0')
    head = data[:pos]
    rest = data[pos+1:]
    return head, rest
