from abc import ABC

import pandas as pd

from ntiles import plotter
from ntiles.tears.base_tear import BaseTear
from ntiles import utils


class ICTear(BaseTear, ABC):
    """
    Computes IC from the given factor and returns
    """

    def __init__(self, factor_data: pd.DataFrame, daily_returns: pd.DataFrame, holding_period: int):
        """
        :param factor_data: factordata to look at
        :param daily_returns: daily returns we are calculating the IC on
        :param holding_period: Holding period we are calculating IC for
        """
        super().__init__()
        self.factor_data = factor_data
        self.daily_returns = daily_returns
        self.holding_period = holding_period

        self.daily_ic = None
        self.ic_stats = None

    def compute(self) -> None:
        """
        runs the functions to compute the IC
        :return: None
        """
        self.compute_ic()
        self.plot_ic()

    #
    # Calculation
    #
    def compute_ic(self) -> None:
        """
        master function for computing the IC
        :return: None
        """""
        self.compute_daily_ic()
        self.calculate_ic_table()

    def compute_daily_ic(self) -> None:
        """
        calculates and sets the daily IC for the holding period
        :return: None
        """
        self.factor_data.index.names = ['date', 'id']

        # slicing off factor values we dont have forward return data for
        factor_unstacked = self.factor_data['factor'].unstack().iloc[:-self.holding_period]
        forward_returns = self.compute_forward_returns().reindex_like(factor_unstacked)

        ic_array = utils.correlation_2d(factor_unstacked.to_numpy(), forward_returns.to_numpy())
        self.daily_ic = pd.Series(ic_array, index=forward_returns.index)

    def compute_forward_returns(self) -> pd.DataFrame:
        """
        Calculates self.holding_period forward returns from daily returns
        :return: index: date; columns: asset; values: self.holding_period forward returns
        """
        # must mad extra day due to cumprod making first date nan
        daily_ret = self.daily_returns  # utils.pad_extra_day(self.daily_returns, 0)
        return daily_ret.add(1).cumprod().pct_change(self.holding_period).shift(-self.holding_period)

    def calculate_ic_table(self) -> None:
        """
        calculates summary stats for the IC data
        :return: None, sets self.ic_stats
        """
        mean_ic = self.daily_ic.mean()
        std_ic = self.daily_ic.std()
        stats = {
            'IC Mean': mean_ic,
            'IC Std': std_ic,
            'Risk Adjusted IC': mean_ic / std_ic,
            'IC Skew': self.daily_ic.skew()
        }

        self.ic_stats = pd.Series(stats).round(3).to_frame(f'{self.holding_period}D').transpose()

    #
    # Plotting
    #
    def plot_ic(self) -> None:
        """
        plots the IC data in self.daily_ic
        :return: None
        """
        print('Information Coefficient')
        plotter.render_table(self.ic_stats)
        plotter.plot_timeseries_ic(self.daily_ic, self.holding_period)
        # plotter.plot_ic_qq(self.daily_ic)
        # plotter.plot_ic_hist(self.daily_ic)

    #
    # To clipboard functions
    #
    def ic_to_clipboard(self) -> None:
        """
        writes ic to the clipboard
        :return: None
        """
        self.daily_ic.to_clipboard()
