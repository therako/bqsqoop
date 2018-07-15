"""Typed module has useful functions to validate types

examples:

"""
from bqsqoop.utils.errors import (
    MissingDataError, InvalidTypeError
)


def non_empty_string(data):
    if data is None or data == "":
        return MissingDataError()
    elif type(data) != str:
        return InvalidTypeError("string")
