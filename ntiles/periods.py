#
# Taken from https://github.com/Alexd14/empyrical/blob/master/empyrical/periods.py
#
from typing import Union

import pandas as pd

APPROX_BDAYS_PER_MONTH = 21
APPROX_BDAYS_PER_YEAR = 252

MONTHS_PER_YEAR = 12
WEEKS_PER_YEAR = 52
QTRS_PER_YEAR = 4

DAILY = 'daily'
WEEKLY = 'weekly'
MONTHLY = 'monthly'
QUARTERLY = 'quarterly'
YEARLY = 'yearly'

PANDAS_PERIOD_TO_PERIOD_STRING = {
    'D': DAILY,
    'W': WEEKLY,
    'M': MONTHLY,
    'Q': QUARTERLY,
    'Y': YEARLY
}

ANNUALIZATION_FACTORS = {
    DAILY: APPROX_BDAYS_PER_YEAR,
    WEEKLY: WEEKS_PER_YEAR,
    MONTHLY: MONTHS_PER_YEAR,
    QUARTERLY: QTRS_PER_YEAR,
    YEARLY: 1
}


def get_period_string(dates: Union[pd.PeriodIndex, pd.Series]) -> str:
    """
    Gets the string definition of a period from a pandas.PeriodIndex or pandas Series
    :param dates: Pandas period index or columns of period we are getting the frequency for
    :return: a period string defined above
    """
    if isinstance(dates, pd.Series):
        dates = dates.dt

    freq = dates.freq.name
    if freq not in PANDAS_PERIOD_TO_PERIOD_STRING:
        raise ValueError(f'Unknown frequency: {freq}')

    return PANDAS_PERIOD_TO_PERIOD_STRING[freq]


def get_period_annualization(dates: Union[pd.PeriodIndex, pd.Series]) -> int:
    """
    Gets the annualization factor that corresponds to the frequency of the given pandas.PeriodIndex or pandas Series
    :param dates: Pandas period index or columns of period we are getting the frequency for
    :return: The number of observations of the given date frequency in a year
    """
    return ANNUALIZATION_FACTORS[get_period_string(dates)]
