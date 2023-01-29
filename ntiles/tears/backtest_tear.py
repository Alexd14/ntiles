import warnings
from abc import ABC

import numpy as np
import pandas as pd

from ntiles.tears.base_tear import BaseTear
from ntiles import plotter, stats, utils


class BacktestTear(BaseTear, ABC):
    """
    Computes returns and stats from the given factor and pricing data

    Upgrades:
        Have cash account for when security gets delisted and we own it
        One day holding period

    """

    def __init__(self, ntile_matrix: pd.DataFrame, daily_returns: pd.DataFrame, ntiles, holding_period: int,
                 long_short: bool, market_neutral: bool, show_uni: bool):
        """
        :param ntile_matrix: unstacked and formatted ntiles prepared by Ntiles
        :param daily_returns: unstacked and formatted daily returns from Ntiles
        :param holding_period: How long we want to hold positions for, represents days
        :param ntiles: amount of bins we are testing (1 is high factor value n is low value)
        :param long_short: show we compute the spread between ntiles: (1 - n)
        :param market_neutral: subtract out the universe returns from the ntile returns?
        :param show_uni: suhould universe return be shown in the spread plot?
        """

        super().__init__()

        self.ntile_matrix = ntile_matrix
        self.daily_returns = daily_returns
        self.ntiles = ntiles
        self.holding_period = holding_period
        self.long_short = long_short
        self.market_neutral = market_neutral
        self.show_uni = show_uni

        self.daily_weights = {}
        self.weighted_returns = {}
        self._daily_tile_returns = None

    def compute(self) -> None:
        """
        method to run the backtest
        """
        self.kick_backtest()

    def plot(self):
        """
        method to plot the data for the backtest
        """
        self.kick_visuals()

    #
    # Vectorized Ntile Backtest
    #
    def kick_backtest(self):
        """
        Calculates the daily returns of each ntile
        Saves the daily returns in self._daily_tile_returns
            index: pd.Period
            columns: Ntile: {ntile}
            Values: Daily close ntile returns on corresponding day
        :return: None
        """

        daily_ntile_returns = self._get_ntile_returns_helper()

        if self.long_short:
            daily_ntile_returns[f'1 vs {self.ntiles}'] = (daily_ntile_returns.iloc[:, 0] -
                                                          daily_ntile_returns.loc[:, f'Ntile: {self.ntiles}']) / 2

            if self.ntiles > 3:
                daily_ntile_returns[f'2 vs {self.ntiles - 1}'] = (daily_ntile_returns.iloc[:, 1] -
                                                                  daily_ntile_returns.loc[:,
                                                                  f'Ntile: {self.ntiles - 1}']) / 2

        self._daily_tile_returns = daily_ntile_returns

    def _get_ntile_returns_helper(self) -> pd.DataFrame:
        """
        Helper to get the returns for each ntile on each day
        :return: data frame index: pd.period; columns: Ntile; values: daily returns
        """
        np_ntile_matrix = self.ntile_matrix.to_numpy()
        np_asset_returns_matrix = self.daily_returns.to_numpy()

        out = {}
        for ntile in range(1, self.ntiles + 1):
            out[f'Ntile: {ntile}'] = self._compute_daily_ntile_returns(np_ntile_matrix, np_asset_returns_matrix, ntile,
                                                                       self.holding_period)

        universe_ntile_matrix = np.where(np.isfinite(np_ntile_matrix), 1, np.nan)[self.holding_period - 1:]
        universe_returns_matrix = np_asset_returns_matrix[self.holding_period - 1:]

        out['universe'] = self._compute_daily_ntile_returns(universe_ntile_matrix, universe_returns_matrix, 1, 1)

        if self.holding_period != 1:
            out = pd.DataFrame(out, index=self.ntile_matrix.index[self.holding_period - 2:])
        else:
            raise ValueError('One day holding period is currently not supported!')
            #print()
            #out = pd.DataFrame(out, index=self.ntile_matrix.index)

        if self.market_neutral:
            # subtracting out universe returns
            ntile_cols = utils.get_ntile_cols(out)
            out.loc[:, ntile_cols] = out.loc[:, ntile_cols].subtract(out['universe'], axis=0)

        if not self.show_uni:
            out.drop('universe', axis=1, inplace=True)

        return out

    def _compute_daily_ntile_returns(self, ntile_matrix: np.array, asset_returns_matrix: np.array, ntile: int,
                                     holding_period: int) -> np.array:
        """
        Computes the daily returns for a ntile
        :param ntile_matrix: the matrix of ntiles
        :param asset_returns_matrix: the matrix for returns
        :param ntile: the amount of ntiles we have computed
        :param holding_period: how long we are holding the assets for
        :return: 1d np.array of the daily return for the ntile
        """

        #
        # Calculating the asset weight per day
        #
        weight_per_day = 1 / np.count_nonzero(ntile_matrix == ntile, axis=1) / holding_period
        if (weight_per_day > .05).any():
            warnings.warn('We have a asset with daily weight over 5%\nLimiting weight at 5%')
            weight_per_day = np.minimum(weight_per_day, np.full(weight_per_day.shape, .05))

        raw_daily_weights = np.where(ntile_matrix == ntile, np.expand_dims(weight_per_day, axis=1), 0)
        daily_weights = utils.rolling_sum(raw_daily_weights, holding_period)

        weighted_asset_returns = daily_weights * asset_returns_matrix[holding_period - 1:, :]
        daily_returns = np.insert(np.sum(weighted_asset_returns, axis=1), 0, 0)

        self.record_backtest_components(ntile, daily_weights, weighted_asset_returns)

        return daily_returns

    def record_backtest_components(self, ntile, daily_weights, weighted_asset_returns):
        """
        records the components to compute the backtest for a specific ntile
        :param ntile: the ntile the data is for
        :param daily_weights: the weights of each asset on the corresponding day
        :param weighted_asset_returns: the weighted returns of each asset
        :return: None
        """
        self.daily_weights[f'Ntile: {ntile}'] = \
            pd.DataFrame(daily_weights, index=self.daily_returns.index[self.holding_period - 1:],
                         columns=self.daily_returns.columns)

        self.weighted_returns[f'Ntile: {ntile}'] = \
            pd.DataFrame(weighted_asset_returns, index=self.daily_returns.index[self.holding_period - 1:],
                         columns=self.daily_returns.columns)

    #
    # Visuals
    #
    def kick_visuals(self) -> None:
        """
        controls displaying visuals to the user
        :return: None
        """
        print('Ntile Backtest')
        cum_ret = stats.cum_returns(self._daily_tile_returns)

        # ntile stats
        ntile_cols = utils.get_ntile_cols(self._daily_tile_returns)
        ntile_daily_ret = self._daily_tile_returns[ntile_cols]
        ntile_cum_ret = cum_ret[ntile_cols]
        avg_annual_ret = stats.CAGR(ntile_cum_ret)
        # ntile plotting
        stats.generate_return_stats(ntile_daily_ret, self.market_neutral)
        freq = ntile_cum_ret.index.freq.name
        plotter.ntile_return_plot(ntile_cum_ret, f'Ntile Returns {self.holding_period}{freq} Holding Period')
        plotter.ntile_annual_return_bars(avg_annual_ret, self.holding_period, freq)

        if self.long_short:
            # spread stats
            spread_cols = utils.get_non_ntile_cols(self._daily_tile_returns)
            long_short_frame = self._daily_tile_returns[spread_cols]
            # spread plotting
            stats.generate_return_stats(long_short_frame, False)
            plotter.ntile_return_plot(cum_ret[spread_cols],
                                      f'Long Short Returns {self.holding_period}{freq} Holding Period')

    #
    # Data methods
    #
    def cum_ret_to_clipboard(self) -> None:
        """
        write cumulative returns to clipboard
        :return: None
        """
        stats.cum_returns(self._daily_tile_returns).to_clipboard()
