from pandas.tslib import Timestamp
from datetime import datetime
from ..helper.data_helper import round_to_nearest_hour


def test_round_nearest_hour():
    ts1 = Timestamp("2015-12-04 10:15:00")
    ts2 = Timestamp("2015-12-04 10:31:00")
    ts3 = Timestamp("2015-12-31 23:35:00")
    ts4 = Timestamp("2016-1-1 00:05:00")

    assert round_to_nearest_hour(ts1) == datetime(2015, 12, 4, 10, 0)
    assert round_to_nearest_hour(ts2) == datetime(2015, 12, 4, 11, 0)
    assert round_to_nearest_hour(ts3) == datetime(2016, 1, 1, 0, 0)
    assert round_to_nearest_hour(ts4) == datetime(2016, 1, 1, 0, 0)
