import asyncio
import struct
from .consts import REQ


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

    header = REQ + struct.pack(">II", packet_type, len(buf))
    return header + buf


class JobHandle:


    def __init__(self, function, data, unique_id, job_id, loop):
        self._function = function
        self._data = data
        self._unique_id = unique_id

        self._job_id= job_id

        self._work_data = []
        self._work_warnings = []

        self._loop = loop
        self._fut = asyncio.Future(loop=self._loop)

    def _add_result(self, data):
        self._work_data.append(data)

    def _notify(self):
        self._fut.set_result(self._work_data)

    def wait_result(self):
        return self._fut

    @property
    def function(self):
        return self._function

    @property
    def data(self):
        return self._data

    @property
    def unique_id(self):
        return self._unique_id

    @property
    def job_id(self):
        return self._job_id

    @property
    def result(self):
        if not self._fut.done():
            raise Exception('Wait for job completion with yf')
        return self._work_data