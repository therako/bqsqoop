import math

from datetime import datetime, timedelta


def _calculate_splits_for_int(min_value, max_value, n):
    count_per_batch = int(
        math.ceil((max_value - min_value + 1) / (n * 1.0)))
    splits = []
    for i in range(n):
        splits.append({
            'start': (count_per_batch * (i) + min_value),
            'end': ((count_per_batch * (i) + min_value) + count_per_batch - 1)
        })
    return splits


def _calculate_splits_for_timestamp(min_value, max_value, n):
    diff_secs = (max_value - min_value).total_seconds()
    count_per_batch = int(math.ceil((diff_secs + 1) / (n * 1.0)))
    splits = []
    for i in range(n):
        splits.append({
            'start': (min_value + timedelta(seconds=(count_per_batch * i))),
            'end': (
                (timedelta(seconds=(count_per_batch * i)) + min_value) +
                timedelta(seconds=(count_per_batch)))
        })
    return splits


_split_fn_map = {
    int: _calculate_splits_for_int,
    datetime: _calculate_splits_for_timestamp
}


def calculate_splits(min_value, max_value, n):
    """Splits and returns n ranges between min_value and max_value

    Supports both int and timestamp type values.

    """
    fn = _split_fn_map.get(type(min_value))
    if fn:
        return fn(min_value, max_value, n)
    else:
        raise Exception("Type of the split-by column is not supported.")
