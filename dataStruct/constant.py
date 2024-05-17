from typing import Final
from decimal import Decimal


class Constant:
    ZERO_DECIMAL: Final = Decimal(0)
    DAYS_IN_MILL: Final = 86400000
    DAYS_IN_MILL_DECIMAL: Final = Decimal(DAYS_IN_MILL)

    EQUAL: Final = "EQ"
    NOT_EQUAL: Final = "NE"
    GREATER_THAN: Final = "GT"
    GREATER_THAN_AND_EQUAL: Final = "GTE"
    LESS_THAN: Final = "LT"
    LESS_THAN_AND_EQUAL: Final = "LTE"

    OT_CALL: Final = "CALL"
    OT_PUT: Final = "PUT"

    # Precision
    PRICE_PRECISION: Final = Decimal("1.0000000000000000")
    GREEKS_PRECISION: Final = Decimal("1.000000000000000")
    VALUE_PRECISION: Final = Decimal("1.0000000000000000")
    RATE_PRECISION: Final = Decimal("1.0000")

    # API records limitation
    RECORD_NUMBER_LIMITED_PER_REQUEST: Final = 1000

    # Time unit presentation
    ONE_DAY_IN_MILLI: Final = 86400000
    ONE_HOUR_IN_MILLI: Final = 3600000
    ONE_MINUTE_IN_MILLI: Final = 60000
    ONE_DAY_IN_MINUTE: Final = 1440
    ONE_YEAR_IN_DAY: Final = 365
    ONE_THOUSAND_MINUTE_IN_MILLI: Final = 60000000
    ONE_SECOND_IN_MILLI: Final = 1000

    ONE_DAY_IN_HOUR: Final = 24
    ONE_WEEK_IN_DAY: Final = 7

    CURRENT_PRICE_INTERVAL: Final = "1m"

    MIN_COVERAGE: Final = -100
    MAX_COVERAGE: Final = 500

    # eps
    EPS = Decimal("2.220446049250313080847263336181640625E-16")
