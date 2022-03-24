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
        self._turnover = None
        self._summary_stats = dict()

    def compute(self) -> None:
        """
        calculates the data for the tear
        """
        self.calculate_autocorrelation()
        self.calculate_turnover()
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
        factor_unstacked = self._factor_data['factor'].unstack()
        auto_corr_arr = utils.correlation_2d(factor_unstacked.to_numpy(),
                                             factor_unstacked.shift(self._holding_period).to_numpy())

        self._auto_corr = pd.Series(auto_corr_arr, index=factor_unstacked.index)

    def calculate_turnover(self):
        """
        Calculates the turnover of the top and bottom bin with a lag of self._holding_period

        calculates the turnover of n and n - holding period
        """
        # getting frame of only the top and bottom bin
        max_ntile = self._factor_data['ntile'].max()
        turnover_frame = self._factor_data[self._factor_data['ntile'].isin([1, max_ntile])][['ntile']]

        turnover_frame['ntile_shifted'] = turnover_frame['ntile'].unstack().shift(self._holding_period).stack()
        turnover_frame['changed'] = turnover_frame['ntile'] != turnover_frame['ntile_shifted']

        final_turnover = turnover_frame.groupby(['date', 'ntile']).changed.agg(sum=sum, count=len)
        self._turnover = (final_turnover['sum'] / final_turnover['count']).unstack()

    def calculate_summary_stats(self) -> None:
        """
        sets the summary stats for the autocorelation and the turnover
        """
        self._summary_stats['auto'] = self._auto_corr.agg(
            {'Mean AC': np.mean, 'Median AC': np.median, 'Std AC': np.std}).round(3).to_frame(
            f'{self._holding_period}D').transpose()

        self._summary_stats['turnover'] = self._turnover.stack().groupby('ntile').agg(
            **{'Mean Turnover': np.mean, 'Median Turnover': np.median, 'Std Turnover': np.std}).round(3)

    def plot_turnover(self) -> None:
        """
        plots the time series data in self.auto_corr
        """
        print('Autocorrelation')
        plotter.render_table(self._summary_stats['auto'])
        plotter.plot_auto_corr(self._auto_corr, self._holding_period)

        print('Turnover')
        plotter.render_table(self._summary_stats['turnover'])
        plotter.plot_turnover(self._turnover, self._holding_period)
