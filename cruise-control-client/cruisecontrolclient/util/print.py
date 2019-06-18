import sys


def print_error(*args, sep=' ', end='\n'):
    """
    Like the print() builtin, but presumes printing to stderr, rather than stdout
    :param sep:   string inserted between values, default a space.
    :param end:   string appended after the last value, default a newline.
    :return:
    """
    print(*args, sep=sep, end=end, file=sys.stderr)
