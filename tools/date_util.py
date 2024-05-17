import math
from datetime import datetime, timedelta
from numbers import Number

from dateutil import tz

from dataStruct.constant import Constant
from tools.granularity_date_util import granularity_milliseconds
from tools.validator import type_validator


def cal_delta_timestamp(timestamp_a, timestamp_b):
    if timestamp_a > timestamp_b:
        tmp = timestamp_b
        timestamp_b = timestamp_a
        timestamp_a = tmp
    delta = timestamp_b - timestamp_a

    return delta/Constant.ONE_DAY_IN_MILLI/Constant.ONE_YEAR_IN_DAY

def timestamp_equal(timestamp_a, timestamp_b, granularity = 'd'):
    dt_a = timestamp_to_datetime(timestamp_a)
    dt_b = timestamp_to_datetime(timestamp_b)

    res = True
    if granularity in ('y', 'm', 'd', 'h'):
        res = res & (dt_a.year == dt_b.year)
        if granularity in ('m', 'd', 'h'):
            res = res & (dt_a.month == dt_b.month)
            if granularity in ('d', 'h'):
                res = res & (dt_a.day == dt_b.day)
                if granularity in ('h'):
                    res = res & (dt_a.hour == dt_b.hour)
    else:
        raise ValueError("Granularity Error.")

    return res



def timestamp_to_datetime(timestamp):
    # transfer timestamp to datetime (Beijing Time)
    return datetime.fromtimestamp(timestamp / 1000, tz=tz.gettz('Asia/Shanghai'))

def timestamp_to_datetime_tz0(timestamp):
    # transfer timestamp to datetime (UTC Time)
    return datetime.fromtimestamp(timestamp / 1000, tz=tz.gettz('UTC'))

def timestamp_to_symbolstr(timestamp, format):
    # formulate timestamp in symbol str, e.g. "25MAR2022"
    dt = timestamp_to_datetime(timestamp)
    symbol = dt.strftime(format)
    return symbol.upper()

def timestamp_to_symbolstr_tz0(timestamp, format):
    # formulate timestamp in symbol str, e.g. "25MAR2022"
    dt = timestamp_to_datetime_tz0(timestamp)
    symbol = dt.strftime(format)
    return symbol.upper()


def date_to_timestamp(year, month, day, hour=0, minute=0, second=0):
    # specify Beijing time to timestamp
    return datetime_to_timestamp(datetime(year, month, day, hour, minute, second, tzinfo=tz.gettz("Asia/Shanghai")))

def date_to_timestamp_tz0(year, month, day, hour=0, minute=0, second=0):
    # specify Beijing time to timestamp
    return datetime_to_timestamp(datetime(year, month, day, hour, minute, second, tzinfo=tz.gettz("UTC")))

def datetime_to_timestamp(r_datetime):
    # transfer datetime to timestamp
    return int(datetime.timestamp( r_datetime ) * 1000)

def datetime_to_timestamp_tz0(r_datetime):
    # First set timezone as UTC0, Second transfer from datetime to timestamp
    return datetime_to_timestamp(r_datetime.replace(tzinfo=tz.gettz('UTC')))

def datetime_floor_to_timestamp(r_datetime, floor_type='s'):
    old_dt = r_datetime
    if floor_type=='m':
        new_dt = old_dt.replace(minute=0, second=0)
    elif floor_type=='s':
        new_dt = old_dt.replace(second=0)
    else:
        new_dt = old_dt
    return datetime_to_timestamp(new_dt)



def timestamp_floor(timestamp, floor_type='s'):
    # m: remove minute and seconds for timestamp
    # s: remove seconds for timestamp
    old_dt = timestamp_to_datetime(timestamp)
    if floor_type=='m':
        new_dt = old_dt.replace(minute=0, second=0)
    elif floor_type=='s':
        new_dt = old_dt.replace(second=0)
    else:
        new_dt = old_dt
    return datetime_to_timestamp(new_dt)
    # return int(np.floor(timestamp / Constant.ONE_HOUR_IN_MILLI) * Constant.ONE_HOUR_IN_MILLI)

def timestamp_output(input):
    if type_validator(input, datetime, False):
        return datetime_to_timestamp(input)
    elif type_validator(input, Number, False):
        return input
    return None

def datetime_plus_deltaday(r_datetime, days=0, hours=0, minutes=0, seconds=0):
    return r_datetime + timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)

def timestamp_plus_deltaday(timestamp, days=0, hours=0, minutes=0, seconds=0):
    dt = datetime_plus_deltaday( timestamp_to_datetime(timestamp), days, hours, minutes, seconds )
    return datetime_to_timestamp( dt )

def generate_timestamp_sequence(starttime: int, endtime: int, granularity: str, seamless: bool = True):
    # generate timestamp sequence: starttime <= t < endtime with granularity
    # t[0] = starttime
    # t[1], t[2], ... can be divided by 15min (e.g. 9:15, 9:30, 9:45, 10:00, ...) when granularity = '15m'
    # assumption:
    # 1. granularity: m, h, d, w
    #       minutes: can be divided by 60, e.g. 10min, 15min
    #       hours: can be divided by 24, e.g. 4h, 6h
    if seamless:
        milli_steps = int(granularity_milliseconds(granularity))
        time_sequence = list( range(starttime, endtime, milli_steps) )
    else:
        time_sequence = [starttime]
        granularity_int = int(granularity[:-1])
        if granularity[-1] == 'm' and 60 % granularity_int == 0:
            start_dt = timestamp_to_datetime(starttime)
            start_dt = datetime(start_dt.year, start_dt.month, start_dt.day, start_dt.hour)
            while datetime_to_timestamp(start_dt) <= starttime:
                start_dt = datetime_plus_deltaday(r_datetime=start_dt, minutes=granularity_int)
            while datetime_to_timestamp(start_dt) < endtime:
                time_sequence.append(datetime_to_timestamp(start_dt))
                start_dt = datetime_plus_deltaday(r_datetime=start_dt, minutes=granularity_int)
        elif granularity[-1] == 'h' and 24 % granularity_int == 0:
            start_dt = timestamp_to_datetime(starttime)
            start_dt = datetime(start_dt.year, start_dt.month, start_dt.day)
            while datetime_to_timestamp(start_dt) <= starttime:
                start_dt = datetime_plus_deltaday(r_datetime=start_dt, hours=granularity_int)
            while datetime_to_timestamp(start_dt) < endtime:
                time_sequence.append(datetime_to_timestamp(start_dt))
                start_dt = datetime_plus_deltaday(r_datetime=start_dt, hours=granularity_int)
        elif granularity[-1] == 'd':
            start_dt = timestamp_to_datetime(starttime)
            start_dt = datetime(start_dt.year, start_dt.month, start_dt.day)
            while datetime_to_timestamp(start_dt) <= starttime:
                start_dt = datetime_plus_deltaday(r_datetime=start_dt, days=granularity_int)
            while datetime_to_timestamp(start_dt) < endtime:
                time_sequence.append(datetime_to_timestamp(start_dt))
                start_dt = datetime_plus_deltaday(r_datetime=start_dt, days=granularity_int)
        elif granularity[-1] == 'w' and granularity_int == 1:
            start_dt = timestamp_to_datetime(starttime)
            start_dt = datetime(start_dt.year, start_dt.month, start_dt.day)
            while datetime_to_timestamp(start_dt) <= starttime or start_dt.weekday()!=0:
                start_dt = datetime_plus_deltaday(r_datetime=start_dt, days=1)
            while datetime_to_timestamp(start_dt) < endtime:
                time_sequence.append(datetime_to_timestamp(start_dt))
                start_dt = datetime_plus_deltaday(r_datetime=start_dt, days=7)
        else:
            raise ValueError('Granularity of %s Error.' % (granularity))

    return time_sequence

def month_to_season(month: int) -> int:
    if month<1 or month>12:
        raise ValueError( f"[ERROR] input {month} month" )
    return math.ceil(month / 3)