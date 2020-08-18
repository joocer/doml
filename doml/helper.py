import os

def reverse_file_read(fp):
    """
    a generator that returns the lines of a file in reverse order
    """

    line = ''
    fp.seek(0, os.SEEK_END)
    offset = fp.tell()

    while offset > 0:
        offset = max(0, offset - 1)
        fp.seek(offset)
        byte = fp.read(1)
        if byte == '\n':
            yield line
            line = ''
        else:
            line = byte + line
    yield line