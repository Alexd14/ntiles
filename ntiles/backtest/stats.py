import empyrical
import pandas as pd
import numpy as np

from . import plotter
from .periods import get_period_annualization, get_period_string


def generate_return_stats(period_returns, flip_mdd) -> None:
    """
    generates following returns statics for each Ntile:
        - Sharpe
        - Annual Return
        - Annual Vol
        - % Periods Up
        - Max Drawdown (flips for top and bottom bins, excluding middle bin)

        If long_short = Ture:
            - All above calculated on spread and universe
            - Annual Tracking Error
            - Information Ratio
    :param period_returns: the returns we are calculating stats for
    :param flip_mdd: should max draw down be flipped around the center?
    """
    ntile_funcs = {
        'sharpe': sharpe_ratio,
        'CAGR': simple_returns_CGAR,
        'Vol': annual_volatility,
        'Max Drawdown': lambda x: max_drawdown(x, flip_mdd),
        '% Periods Up': percent_periods_up,
    }

    calculated_stats = [func(period_returns) for func in ntile_funcs.values()]
    render_me = pd.DataFrame(calculated_stats).transpose()
    plotter.render_heat_table(render_me)


def compute_ntile_stats(name, func, ntile_returns) -> pd.Series:
    """
    apply a function to each column of ntile_returns
    :param name: name of the function
    :param func: the function to apply
    :param ntile_returns: the returns we are applying the function to
    :return: pd.Series, index: Ntile; Name: name;
    """
    return ntile_returns.apply(func, axis=0).rename(name)


def max_drawdown(period_returns, flip_bottom) -> pd.Series:
    """
    computes the max drawdown for each column
    flips the drawdown from downside to upside for ntiles that should be negative
    :param period_returns: the returns we are getting the drawdown for
    :param flip_bottom:
    :return: pd.Series, index: Ntile; Values: drawdown
    """
    adj_ret = period_returns.copy()  # gets rid of setting on copy warning
    num_cols = period_returns.shape[1]

    if flip_bottom:
        mid_pos = int(round(num_cols / 2 + .5)) - 1
        adj_ret.iloc[:, mid_pos:] = adj_ret.iloc[:, mid_pos:] * -1
        out = compute_ntile_stats('Max Drawdown', empyrical.max_drawdown, adj_ret)

        if num_cols % 2 == 1:  # if even number of columns is odd pad null
            out.iloc[mid_pos] = None

        return out * 100

    return compute_ntile_stats('Max Drawdown', empyrical.max_drawdown, adj_ret) * 100


def percent_periods_up(period_returns) -> pd.Series:
    """
    computes the percent of periods where return is > 0
    :param period_returns: the returns we are getting the % of periods up for
    :return: pd.Series, index: Ntile; Values: % periods up
    """
    periods_up = period_returns.copy()
    periods_up.iloc[:] = np.where(period_returns.values > 0, 1, 0)
    return (periods_up.sum(axis=0) / periods_up.shape[0]).rename('% Periods Up')


def annual_volatility(period_returns) -> pd.Series:
    """
    computes the annual volatility of each column
    :param period_returns: the returns we are getting the vol for
    :return: pd.Series, index: Ntile; Values: annual vol
    """
    vol_func = wrap_emprical_period(empyrical.annual_volatility)
    return compute_ntile_stats('Annual Vol', vol_func, period_returns) * 100


def sharpe_ratio(period_returns) -> pd.Series:
    """
    computes the sharpe ratio for each
    :param period_returns: the returns we are getting the sharpe of
    :return:pd.Series, index: Ntile; Values: sharpe
    """
    sharpe_func = wrap_emprical_period(empyrical.sharpe_ratio)
    return compute_ntile_stats('Sharpe', sharpe_func, period_returns)


def simple_returns_CGAR(period_returns) -> pd.Series:
    """
    computes the CAGR form simple returns
    :param period_returns: make t e
    :return: series with index: cum_returns.columns; values: corresponding average return in percent
    """
    return CAGR(cum_returns(period_returns))


def CAGR(cum_returns_df: pd.DataFrame) -> pd.Series:
    """
    calculates the geometric average yearly ntile returns from the given cumulative returns
    Assumed the data is in daily format
    :param cum_returns_df: cn be full cum returns or just the
    :return: series with index: cum_returns_df.columns; values: corresponding average return in percent
    """
    ann_factor = get_period_annualization(cum_returns_df.index)
    return ((cum_returns_df.iloc[-1] ** (1 / (cum_returns_df.shape[0] / ann_factor)) - 1) * 100).rename('CAGR')


def cum_returns(simple_returns: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the daily returns from the simple returns.
    wraps empyrical.cum_returns
    :param simple_returns: returns used to calculate the cumulative returns
    :return: cumulative returns
    """
    return empyrical.cum_returns(simple_returns, starting_value=1)


def wrap_emprical_period(func):
    """
    Wraps and empirical function that takes in returns and period
    :param func: the emprical function to wrap
    :return: function that takes in returns and infers the frequency of the data
        and passes a period to the enmprical fucntion
    """

    def inner_wrapper(period_returns):
        return func(returns=period_returns, period=get_period_string(period_returns.index))

    return inner_wrapper
