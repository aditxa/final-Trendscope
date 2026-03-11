from datetime import datetime, timedelta, timezone

from gdelt.src.gdelt_client import _split_window


def test_split_window_midpoint():
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(hours=2)
    left_start, left_end = _split_window(start, end)

    assert left_start == start
    assert left_end == start + timedelta(hours=1)
