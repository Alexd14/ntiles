import warnings

import pandas as pd

from ntiles.tears.ic_tear import ICTear
from ntiles.tears.inspection_tear import InspectionTear
from ntiles.tears.backtest_tear import BacktestTear

from ntiles.portals.base_portal import BaseGrouperPortalConstant
from ntiles.portals.pricing_portal import PricingPortal
from ntiles.tears.tilts_backtest_tear import TiltsBacktestTear


class Ntile:
    def __init__(self, pricing_portal: PricingPortal, group_portal: BaseGrouperPortalConstant = None):
        self._pricing_portal: PricingPortal = pricing_portal
        self._group_portal = group_portal

        self._factor_data = None
        self._ntile_matrix = None
        self._formatted_returns = None

        # Tear Sheets Below
        self._inspection_tear = None
        self._backtest_tear = None
        self._ic_tear = None

    def _input_checks(self, factor_series) -> None:
        """
        checks the factor series to ensure it meet requirements to runa backtest

        Requirements:
            1) series must have MultiIndex with 2 levels
            2) First level must be of type pd.Period
            3) PricingPortal must have data for all Period dates in the series
            4) There can only be one observations for a single asset on a single day

        :param factor_series: the series we are checking
        :return: None
        :raise ValueError: if one of the requirements are not met
        """

        # checking for series with multi index, possibly also check types for multi index
        if not isinstance(factor_series.index, pd.MultiIndex) or factor_series.index.nlevels != 2:
            raise ValueError('Factor input must have MultiIndex of period, id')

        # ensure the index level zero is date
        if not isinstance(factor_series.index.get_level_values(0), pd.PeriodIndex):  # could also be pd.Period idk
            raise ValueError('Factor input must have MultiIndex with the first level being a period '
                             f'current factor dtype is {type(factor_series.index.get_level_values(0))}')

        # we will check id when looking for overlapping portal names
        no_pricing_for = set(factor_series.index.get_level_values(1)).difference(
            self._pricing_portal.assets)
        if len(no_pricing_for) != 0:
            # raise ValueError(f'PricingPortal does not have data for: {no_pricing_for}')
            warnings.warn(f'PricingPortal does not have data for: {no_pricing_for}')

        # make sure pricing portal dates match up with factor
        overlapping_periods = set(factor_series.index.get_level_values(0).drop_duplicates()).intersection(
            self._pricing_portal.periods)
        if len(overlapping_periods) == 0:
            raise ValueError('No overlap between PricingPortal dates and factor dates')
        if len(overlapping_periods) < 100:
            warnings.warn(f'Only {len(overlapping_periods)} common dates between PricingPortal dates and factor')

        # check for multiple observations on a single day for a single asset
        if factor_series.index.duplicated().any():
            raise ValueError('Multiple factor observations on single day for a single asset')

    def _set_ntiles_and_returns(self, factor_data, ntiles):
        """
        Sets self._formatted_returns and  self._formatted_ntile
        :param factor_data: the factor data
        :param ntiles: amount of ntiles
        :return: None
        """
        self.ntile_factor(factor_data, ntiles)
        self._align_ntiles_pricing()

    def _align_ntiles_pricing(self) -> None:
        """
        ensures ntiled matrix and daily returns matrix have the same column and row order
        sets self._formatted_returns and self._ntile_matrix
        :return: None
        """
        ntile_factor = self._factor_data['ntile'].unstack()
        daily_returns = self._pricing_portal.delta_data

        factor_date = ntile_factor.index.get_level_values('date')
        self._formatted_returns = daily_returns[(daily_returns.index >= factor_date.min()) &
                                                (daily_returns.index <= factor_date.max())]

        # can see what % of the dataframe is null here
        self._ntile_matrix = ntile_factor.reindex_like(self._formatted_returns)

    def ntile_factor(self, factor: pd.Series, ntiles: pd.Series) -> None:
        """
        Universe relative Quantiles of a factor by day

        pd.DataFrame of ntiled factor
            index: (pd.Period, asset_id)
            Columns: (factor, ntile)
            Values: (factor value, Ntile corresponding to factor value)

        :param factor: same var as ntile_return_tearsheet
        :param ntiles: same var as ntile_return_tearsheet
        """
        # add a filter for if a day has less than 20% factor data then just put bin as -1 for all assets
        # unstack the frame, percentile rank each row, divide whole matrix buy 1/ntiles, take the floor of every number
        factor = factor.dropna().to_frame('factor')

        try:
            factor['ntile'] = factor.groupby('date').transform(
                lambda date_data: ntiles - pd.qcut(date_data, ntiles, labels=False)
            )
        except Exception as e:
            print('Hit error while binning data. Need to push the histogram')
            print('Your data is mighty sus we can\'t Ntile it. This is normally due to bad data')

            # forcing a histogram out
            import matplotlib.pyplot as plt
            factor.factor.hist()
            plt.show()

            raise e

        self._factor_data = factor

    #
    # Start up methods
    #
    def _prep_for_run(self, factor: pd.Series, ntiles: int) -> None:
        """
        prepares the ntiles class to run a tear sheet
        :param factor: factor for tear sheet
        :param ntiles: num ntiles for sheet
        :return: None
        """
        factor_series = factor.copy()
        self._input_checks(factor_series)

        factor_series.index.names = ['date', 'id']
        self.kick_tears(factor_series, ntiles)

    def kick_tears(self, factor_series: pd.Series, ntiles: int) -> None:
        """
        Clears the object of all factor and tear data.
        Reruns Ntiling of factor
        :param factor_series: the user passed factor
        :param ntiles: the number of ntiles
        :return: None
        """
        self._clear()
        self._set_ntiles_and_returns(factor_series, ntiles)

    def _clear(self) -> None:
        """
        clears all data points in the object except the pricing portal
        :return: None
        """
        self._factor_data = None
        self._ntile_matrix = None
        self._formatted_returns = None

        # Tear Sheets Below
        self._inspection_tear = None
        self._backtest_tear = None
        self._ic_tear = None

    def _run(self) -> None:
        """
        Runs all tear sheets that are set in the class
        :return: None
        """
        tears = [self._inspection_tear, self._backtest_tear, self._ic_tear]
        for tear in tears:
            if tear:
                tear.compute()

    #
    # Run Decorator
    #
    def _start_up(function):
        """
        A decorator to run the necessary preparation function to run a tearsheet.
        Due to the ntiles function storing the tears as attributes we must clear the attributes on every run.
        If the attributes are not cleared then we would re run every saved tearsheet

        this decorator is a bit jank so could like to eventually change it
        :param function: the tearsheet function we are running
        :return: None
        """

        def start_up_inner(self, factor, ntiles=1, *args, **kwargs):
            self._prep_for_run(factor, ntiles)
            function(self, *args, factor=self._factor_data, ntiles=ntiles, **kwargs)
            self._run()

        return start_up_inner

    def _start_up_no_ntiles(function):
        """
        same as _start_up but is used for functions that done require the ntiles parameter
        probably can just wrap _start_up somehow instead of writing basically the same code over

        :param function: the tearsheet function we are running
        :return: None
        """

        def start_up_inner(self, factor, *args, **kwargs):
            self._prep_for_run(factor, 1)
            function(self, *args, factor=self._factor_data, **kwargs)
            self._run()

        return start_up_inner

    #
    # Tear Sheets Below
    #
    @_start_up
    def full_tear(self, factor: pd.Series, ntiles: int, holding_period: int,
                  long_short: bool = True, market_neutral=True, show_plots=True, show_uni=False,
                  show_ntile_tilts=False) -> None:
        """
        runs all tear sheets
        :param factor: @ntile_backtest_tear | @ntile_inspection_tear
        :param ntiles: @ntile_backtest_tear | @ntile_inspection_tear
        :param holding_period: @ntile_backtest_tear
        :param long_short: @ntile_backtest_tear
        :param market_neutral: @ntile_backtest_tear
        :param show_plots: @ntile_backtest_tear
        :param show_uni: @ntile_backtest_tear
        :param show_ntile_tilts: @ntile_backtest_tear
        :return: None
        """
        self._inspection_tear = InspectionTear(self._factor_data)
        self._backtest_tear = TiltsBacktestTear(self._ntile_matrix, self._formatted_returns, ntiles, holding_period,
                                                long_short, market_neutral, show_plots, show_uni, self._factor_data,
                                                self._group_portal, show_ntile_tilts)
        self._ic_tear = ICTear(self._factor_data, self._formatted_returns, holding_period)

    @_start_up
    def ntile_backtest_tear(self, factor: pd.Series, ntiles: int, holding_period: int,
                            long_short: bool = True, market_neutral=True, show_plots=True, show_uni=False,
                            show_ntile_tilts=False) -> None:
        """
        Creates a fan chart of cumulative returns for the given factor values.
        The factor values are ntile'd into ntiles number of bins

        The in the cumulative return plot, each value represents the cumulative return up to that days close.
        Returns are not shifted each value represents portfolios value on the close of that day.

        A set of weights is generated for each day based off factor quantile.
        The portfolio is rebalanced daily, each days 1/holding_period of the portfolio is rebalanced.
        All positions are equally weighted.

        :param factor: The factor values being tested.
            index: (pd.Period, asset_id)
            values: (factor_value)
        :param holding_period: How long we want to hold positions for, represents days
        :param ntiles: amount of bins we are testing (1 is high factor value n is low value)
        :param long_short: show we compute the spread between ntiles: (1 - n)
        :param market_neutral: subtract out the universe returns from the ntile returns?
        :param show_plots: should stats and plots be shown?
        :return: plots showing the return profile of the factor
        :param show_uni: Should universe return be shown in the spread plot?
        :param show_ntile_weights: should we show each ntiles tilts?
        """
        self._backtest_tear = TiltsBacktestTear(self._ntile_matrix, self._formatted_returns, ntiles, holding_period,
                                                long_short, market_neutral, show_plots, show_uni, self._factor_data,
                                                self._group_portal, show_ntile_tilts)

    @_start_up
    def ntile_inspection_tear(self, factor: pd.Series, ntiles: int) -> None:
        """
        runs InspectionTear
        :param factor: the factor data to inspect
        :param ntiles: the number of ntiles
        :return: None
        """
        self._inspection_tear = InspectionTear(factor)

    @_start_up_no_ntiles
    def ntile_ic_tear(self, factor: pd.Series, holding_period: int):
        """
        runs ICTear
        :param factor: The factor values being tested.
            index: (pd.Period, asset_id)
            values: (factor_value)
        :param holding_period: How long we want to hold positions for, represents days
        :return: None
        """
        self._ic_tear = ICTear(self._factor_data, self._formatted_returns, holding_period)

    _start_up = staticmethod(_start_up)
    _start_up_no_ntiles = staticmethod(_start_up_no_ntiles)

    #
    # get data functions
    #
    def cum_ret_to_clipboard(self) -> None:
        """
        Pastes the cumulative returns to the clipboard.
        Useful for pasting into excel
        :return: None
        """
        if not self._backtest_tear:
            raise ValueError('Have not yet ran a backtest!')

        self._backtest_tear.cum_ret_to_clipboard()

    def ic_to_clipboard(self) -> None:
        """
        Writes ic to the clipboard.
        :return: None
        """
        if not self._ic_tear:
            raise ValueError('Have not yet ran a IC tear yet!')

        self._ic_tear.cum_ret_to_clipboard()

    # @_start_up_no_ntiles
    # def ntile_tilts_tear(self, factor):
    #     """
    #     runs the TiltsTear
    #
    #     does not need normal start up because the
    #     :return: None
    #     """
    #     if self._backtest_tear is None:
    #         raise ValueError('Must runa a ntile_backtest_tear to compute tilts!')
    #
    #     if self._group_portal is None:
    #         raise ValueError('Must have a Group Portal!')
    #
    #     self._tilts_tear = TiltsTear(factor, self._backtest_tear, self._group_portal, True)
