from abc import ABC

from .backtest_tear import BacktestTear
from .. import plotter
from .. import utils

from ntiles.tears.base_tear import BaseTear
from ntiles.portals.base_portal import BaseGrouperPortalConstant


class TiltsTear(BaseTear, ABC):
    """
    generates a tear sheet which shows the sector exposures of a strategy
    Must be run after the backtest tear
    """

    def __init__(self, factor_data, backtest_tear: BacktestTear, group_portal: BaseGrouperPortalConstant,
                 long_short: bool):
        super().__init__()
        self.factor_data = factor_data
        self.backtest_tear = backtest_tear
        self.group_portal = group_portal
        self.long_short = long_short

        self._daily_group_weights = {}
        self._full_group_tilt_avg = {}

    def compute(self) -> None:
        """
        master function for the tear sheet
        :return: None
        """
        if self.group_portal is None:
            return

        self.compute_tilts()
        self.compute_plots()

    def compute_tilts(self):
        """
        computes the daily tilt data for each group
        :return: None
        """
        # self.compute_factor_by_group()
        self.compute_group_weights()
        if self.long_short:
            self.calculate_long_short_tilts()

    def compute_group_weights(self):
        """
        computes the weights by group for each ntile
        currently computes data but work because need a time series data adjusted for index constitutes
        have to use self.factor_data
        :return: None
        """
        group_info = self.group_portal.group_information
        center_weight = group_info.groupby(group_info).count() / group_info.shape[0]
        center_weight = utils.remove_cat_index(center_weight)

        group_col = None
        for ntile in self.backtest_tear.daily_weights.keys():
            frame = self.backtest_tear.daily_weights[ntile].stack().to_frame('weight')

            if group_col is None:
                group_col = frame.index.get_level_values('id').astype(str).map(self.group_portal.group_mapping)

            frame['group'] = group_col

            weights_unstacked = frame.groupby(['date', 'group']).sum().sub(center_weight, level=1, axis=0).unstack()
            weights_unstacked.columns = weights_unstacked.columns.droplevel(0)

            self._daily_group_weights[ntile] = weights_unstacked
            self._full_group_tilt_avg[ntile] = (frame.groupby(['group']).sum().weight
                                                / frame.index.levels[0].unique().shape[0]
                                                - center_weight)

    def calculate_long_short_tilts(self):
        """
        calculates the time series tilts for the long short portfolio
        :return: None
        """
        ntile_n = max(self._daily_group_weights.keys())
        self._daily_group_weights['Long Short'] = (self._daily_group_weights['Ntile: 1']
                                                   - self._daily_group_weights[ntile_n])
        self._full_group_tilt_avg['Long Short'] = self._daily_group_weights['Long Short'].stack().groupby('group').mean()

    def compute_plots(self):
        print('Group Weights By Ntile')
        for ntile in self._daily_group_weights.keys():
            if 'Long Short' == ntile and not self.long_short:
                continue

            ax = plotter.plot_tilt_hist(self._full_group_tilt_avg[ntile], ntile, self.group_portal.name)
            plotter.plot_tilts(self._daily_group_weights[ntile], ntile, self.group_portal.name, ax)
