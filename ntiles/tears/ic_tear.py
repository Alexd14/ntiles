from abc import ABC
from typing import Iterable

import pandas as pd

from ntiles import plotter
from ntiles.tears.base_tear import BaseTear
from ntiles import utils


class ICTear(BaseTear, ABC):
    """
    Computes IC from the given factor and returns

    Currently will only measure IC for days a company is in the universe
    Example: AAPl is in the univere on 1/10 but not in universe on 11/10 if we have greater than 10 day holding period
        that asset wint count in the IC calculation
    """

    def __init__(self, factor_data: pd.DataFrame, daily_returns: pd.DataFrame, holding_period: int):
        """
        :param factor_data: factor data to look at must be from Ntiles
        :param daily_returns: daily returns we are calculating the IC on must be from Ntiles
        :param holding_period: Holding period we are calculating IC for
        """
        super().__init__()
        self.factor_data = factor_data
        self.daily_returns = daily_returns
        self.holding_period = holding_period

        self.daily_ic = None
        self.ic_stats = None

    #
    # Calculation
    #
    def compute(self) -> None:
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
        factor_unstacked = self.factor_data['factor'].unstack()#.iloc[:-self.holding_period]
        forward_returns = self.compute_forward_returns().reindex_like(factor_unstacked)

        ic_array = utils.correlation_2d(factor_unstacked.to_numpy(), forward_returns.to_numpy())
        self.daily_ic = pd.Series(ic_array, index=forward_returns.index).to_frame('IC')
        if self.daily_ic.index.freq.name == 'D':
            self.daily_ic['1 Month Avg IC'] = self.daily_ic.rolling(21).mean()
        else:
            self.daily_ic['1 Year Avg IC'] = self.daily_ic.rolling(12).mean()

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
        mean_ic = self.daily_ic['IC'].mean()
        std_ic = self.daily_ic['IC'].std()
        stats = {
            'IC Mean': mean_ic,
            'IC Median': self.daily_ic['IC'].median(),
            'IC Std': std_ic,
            'Risk Adjusted IC': mean_ic / std_ic,
            'IC Skew': self.daily_ic['IC'].skew()
        }

        self.ic_stats = pd.Series(stats).round(3).to_frame(f'{self.holding_period}D').transpose()

    #
    # Plotting
    #
    def plot(self) -> None:
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


class ICHorizonTear(BaseTear, ABC):
    """
    Computes the IC horizon tear
    Will give insight into optimal holding periods for the factor
    """

    def __init__(self, factor_data: pd.DataFrame, daily_returns: pd.DataFrame, intervals: Iterable[int],
                 show_individual):
        """
        :param factor_data: The factor values being tested, must be from Ntiles
        :param daily_returns: matrix of returns from Ntiles
        :param intervals: an iterable that contains the holding periods we would like to make the IC frontier for
        """
        super().__init__()
        self._factor_data = factor_data
        self._daily_returns = daily_returns
        self._intervals = sorted(list(intervals))
        self._show_individual = show_individual

        self.tears = {}
        self._ic_horizon = None

    def compute(self) -> None:
        """
        runs a IC tear for all the periods we want to test over
        """
        for interval in self._intervals:
            self.tears[interval] = ICTear(self._factor_data, self._daily_returns, interval)
            self.tears[interval].compute()

        self._ic_horizon = pd.concat([tear.ic_stats for tear in self.tears.values()])

    def plot(self) -> None:
        """
        plots the IC frontier and the Time series IC
        """
        plotter.plot_ic_horizon(self._ic_horizon.drop(['IC Skew'], axis=1))
        plotter.render_table(self._ic_horizon)
        if self._show_individual:
            for ic_tear in self.tears.values():
                ic_tear.plot()
