from dataStruct.constant import Constant
from tools.validator import type_validator


def granularity_annualized_const(granularity):
    # output the annualized constant for a given granularity, e.g.
    #   "1h" -> 24*365,
    #   "1d" -> 365
    if not type_validator(granularity, str, nullable=False):
        raise ValueError('Parameter of type str is required')

    granularity_int = int(granularity[:-1])

    annu_const = 1.0
    match granularity[-1]:
        case 'm':
            annu_const *= Constant.ONE_DAY_IN_MINUTE * Constant.ONE_YEAR_IN_DAY
        case 'h':
            annu_const *= Constant.ONE_DAY_IN_HOUR * Constant.ONE_YEAR_IN_DAY
        case 'd':
            annu_const *= Constant.ONE_YEAR_IN_DAY
        case 'w':
            annu_const *= Constant.ONE_YEAR_IN_DAY / Constant.ONE_WEEK_IN_DAY
        case _:  # Pattern not attempted
            raise ValueError('Granularity Error.')

    annu_const /= granularity_int

    return annu_const


def granularity_milliseconds(granularity):
    # output milliseconds contained in a granularity, e.g.
    #   "1h" -> 3600*1000
    annu_const = granularity_annualized_const(granularity)
    return Constant.ONE_YEAR_IN_DAY * Constant.ONE_DAY_IN_MILLI / annu_const

def granularity_units(granularity, resolution='s'):
    # output numbers of time units in a granularity, e.g.
    #   '1h', 's' -> 3600
    #   '15m', 'm' -> 15
    #   '1w', 'y' -> 7 / 365
    #   '1h', 'd' -> 1 / 24
    units = granularity_milliseconds(granularity)

    match resolution:
        case 's':
            units /=  Constant.ONE_SECOND_IN_MILLI
        case 'm':
            units /= Constant.ONE_MINUTE_IN_MILLI
        case 'h':
            units /= Constant.ONE_HOUR_IN_MILLI
        case 'd':
            units /= Constant.ONE_DAY_IN_MILLI
        case 'y':
            units /= Constant.ONE_YEAR_IN_DAY * Constant.ONE_DAY_IN_MILLI
        case _:  # Pattern not attempted
            raise ValueError('Resolution Error.')

    return units


