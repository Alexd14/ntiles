from typing import Union, List

from abc import ABC

import pandas as pd
import numpy as np

from .base_tear import BaseTear
from .. import plotter, utils


class TurnoverTear(BaseTear, ABC):
    """
    Shows the turnover for a factor
    """

    def __init__(self, factor_data: pd.DataFrame, holding_period: Union[int, List[int]]):
        super().__init__()
        self._factor_data = factor_data
        self._holding_period = holding_period

        self._auto_corr = None
        self._summary_stats = None

    def compute(self) -> None:
        """
        calculates the data for the tear
        """
        self.calculate_autocorrelation()
        self.calculate_summary_stats()

    def plot(self) -> None:
        """
        plots the tear
        """
        self.plot_turnover()

    def calculate_autocorrelation(self) -> None:
        """
        Calculates the auto correlation of the factor with a lag of self._holding_period

        calculates the autocorrelation of n and n - holding period
        """
        factor_unstacked = self._factor_data.unstack()
        auto_corr_arr = utils.correlation_2d(factor_unstacked.to_numpy(),
                                             factor_unstacked.shift(self._holding_period).to_numpy())

        self._auto_corr = pd.Series(auto_corr_arr, index=factor_unstacked.index)

    def calculate_summary_stats(self) -> None:
        """
        sets the summary stats for the turnover
        """
        self._summary_stats = self._auto_corr.agg(
            {'Mean AC': np.mean, 'Median AC': np.median, 'Std AC': np.std}).round(3).to_frame(
            f'{self._holding_period}D').transpose()

    def plot_turnover(self) -> None:
        """
        plots the time series data in self.auto_corr
        """
        print('Turnover')
        plotter.render_table(self._summary_stats)
        plotter.plot_auto_corr(self._auto_corr, self._holding_period)
