import warnings
from typing import Dict, Iterable, Optional

import pandas as pd
import duckdb

from .portals.base_portal import BaseGrouperPortalConstant
from .portals.pricing_portal import PricingPortal
from .tears.base_tear import BaseTear
from .tears.ic_tear import ICHorizonTear, ICTear
from .tears.inspection_tear import InspectionTear
from .tears.tilts_backtest_tear import TiltsBacktestTear
from .tears.turnover_tear import TurnoverTear


class Ntile:
    def __init__(self, pricing_portal: PricingPortal, group_portal: Optional[BaseGrouperPortalConstant] = None):
        """
        :param pricing_portal: the pricing portal which holds pricing data for all assets with factor values
        :param group_portal: group portal which holds grouping information for all assets with factor values
            if this is None then no group statistics will be calculated
        """
        self._pricing_portal: PricingPortal = pricing_portal
        self._group_portal = group_portal

        self._factor_data = None
        self._ntile_matrix = None
        self._formatted_returns = None

    def _input_checks(self, factor_series) -> None:
        """
        checks the factor series to ensure it meet requirements to run a tearsheet

        Requirements:
            1) series must have MultiIndex with 2 levels
            2) First level must be of type pd.Period
            3) PricingPortal must have data for all Period dates in the series
            4) There can only be one observations for a single asset on a single day
            4) The factor and pricing have to have the same freq

        :param factor_series: the series we are checking
        :return: None
        :raise ValueError: if one of the requirements are not met
        """

        # checking for series with multi index, possibly also check types for multi index
        if not isinstance(factor_series.index, pd.MultiIndex) or factor_series.index.nlevels != 2:
            raise ValueError('Factor input must have MultiIndex of period, id')

        # ensure the index level zero is date
        if not isinstance(factor_series.index.get_level_values(0), pd.PeriodIndex):
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
            warnings.warn(f'Only {len(overlapping_periods)} common periods between PricingPortal and factor')

        # check for multiple observations on a single day for a single asset
        if factor_series.index.duplicated().any():
            raise ValueError('Multiple factor observations on single day for a single asset')

        # check the pricing and factor freq are the same
        if factor_series.index.get_level_values('date').freq != self._pricing_portal.delta_data.index.freq:
            raise ValueError('Factor and pricing dont have the same freq!')

    def _set_ntiles_and_returns(self, factor_data: pd.Series, ntiles: int):
        """
        Sets self._formatted_returns and  self._formatted_ntile
        :param factor_data: the factor data
        :param ntiles: amount of ntiles
        :return: None
        """
        self._ntile_factor_sql(factor_data, ntiles)
        self._align_ntiles_pricing()

        # can see what % of the dataframe is null here
        self._make_null_summary(factor_data)

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

        # reindexing the ntiles data so that you have pricing and ntiles matching up
        self._ntile_matrix = ntile_factor.reindex_like(self._formatted_returns)

    def _make_null_summary(self, raw_factor_data) -> None:
        """
        making a summary of how much factor data we matched to pricing data
        :param raw_factor_data: the raw unstacked factor data
        """
        length_og_factor_data = len(raw_factor_data)
        # seeing what % of factor data is missing
        num_na_data_points = raw_factor_data.isnull().sum()
        pct_na_data_points = num_na_data_points / length_og_factor_data

        # amount of data droped because of non aligned factor and returns dates:
        # above should be non null length of ntiles before reindexing
        # non null length of ntiles after indexing
        number_of_finite_ntiles = length_og_factor_data - num_na_data_points
        binary_if_ntile_data = self._ntile_matrix.notnull()
        number_of_finite_ntiles_no_overlap_returns = number_of_finite_ntiles - binary_if_ntile_data.sum().sum()
        pct_missing_ntile_no_overlap = number_of_finite_ntiles_no_overlap_returns / number_of_finite_ntiles

        # amount of data we dont have returns for given we have overlapping pricing and factor
        # should ffill ntile by holdign period since we need return data holding_period days out
        binary_if_return_data = self._formatted_returns.notnull()
        # should forward fill by holding period to make sure we have pricing for when we will be holding the stock
        missing_from_no_returns_given_overlap = (number_of_finite_ntiles
                                                 - (binary_if_ntile_data * binary_if_return_data).sum().sum())
        pct_missing_data_no_returns_given_overlap = missing_from_no_returns_given_overlap / number_of_finite_ntiles

        # total number of unusable factor data points due to null or no maped returns
        num_bad = (num_na_data_points
                   + number_of_finite_ntiles_no_overlap_returns
                   + missing_from_no_returns_given_overlap
                   )

        pct_bad = num_bad / length_og_factor_data

        print(f"Unusable Factor Data:           {(round(pct_bad, 4)) * 100}%")
        print(f"NA Factor Values:               {(round(pct_na_data_points, 4)) * 100}%")
        print(f"No Overlapping Returns:         {(round(pct_missing_ntile_no_overlap, 4)) * 100}%")
        print(f"Missing Returns Given Overlap:  {(round(pct_missing_data_no_returns_given_overlap, 4)) * 100}%")

    def _ntile_factor(self, factor: pd.Series, ntiles: int) -> None:
        """
        This is slow replaced by
        Universe relative Quantiles of a factor by day _ntile_factor_sql

        pd.DataFrame of ntiled factor
            index: (pd.Period, _asset_id)
            Columns: (factor, ntile)
            Values: (factor value, Ntile corresponding to factor value)

        :param factor: same var as ntile_return_tearsheet
        :param ntiles: same var as ntile_return_tearsheet
        """
        # add a filter for if a day has less than 20% factor data then just put bin as -1 for all assets
        # unstack the frame, percentile rank each row, divide whole matrix buy 1/ntiles, take the floor of every number
        factor = factor[~factor.isnull()].to_frame('factor')

        try:
            factor['ntile'] = factor.groupby('date').transform(
                lambda date_data: ntiles - pd.qcut(date_data, ntiles, labels=False)
            ).sort_index()
        except Exception as e:
            print('Hit error while binning data. Need to push the histogram')
            print('Your data is mighty sus we can\'t Ntile it. This is normally due to bad data')

            # forcing a histogram out
            import matplotlib.pyplot as plt
            factor.groupby('date').count().plot()
            plt.show()

            raise e

        self._factor_data = factor

    def _ntile_factor_sql(self, factor: pd.Series, ntiles: int) -> None:
        """
        Universe relative Quantiles of a factor by day
        Around 100X faster than pandas groupby qcut

        pd.DataFrame of ntiled factor
            index: (pd.Period, _asset_id)
            Columns: (factor, ntile)
            Values: (factor value, Ntile corresponding to factor value)

        :param factor: same var as ntile_return_tearsheet
        :param ntiles: same var as ntile_return_tearsheet
        """
        factor_freq = factor.index.get_level_values('date').freq
        factor = factor.to_frame('factor').reset_index()
        factor['date'] = factor['date'].dt.to_timestamp()

        sql_quantile = f"""SELECT *, NTILE({ntiles}) OVER(PARTITION BY date ORDER BY factor.factor DESC) as ntile
                            FROM factor
                            WHERE factor.factor IS NOT NULL"""
        con = duckdb.connect(':memory:')
        factor = con.execute(sql_quantile).df()
        factor['date'] = factor['date'].dt.to_period(freq=factor_freq)
        factor = factor.set_index(['date', 'id'])

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
        # checking to see if we have series or data frame
        if isinstance(factor, pd.DataFrame):
            if factor.shape[1] > 1:  # there is a df passed with multible columns
                raise ValueError('There are multiple columns in the passed DataFrame')

            factor_series = factor.iloc[:, 0]
        else:
            factor_series = factor.copy()

        self._input_checks(factor_series)

        factor_series.index.names = ['date', 'id']
        self.kick_tears(factor_series, ntiles)

        self._print_start_end_dates()

    def _print_start_end_dates(self):
        """
        prints the start and end date of the backtest
        """
        date = self._factor_data.index.get_level_values(0)
        print(f'\nStart Date: {date.min()}')
        print(f'End Date:   {date.max()}\n')

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

    @staticmethod
    def _run(tears: Dict[str, BaseTear]) -> None:
        """
        Runs all tear sheets that are set in the class
        :return: None
        """
        for tear in tears.values():
            tear.compute_plot()

    #
    # Tear Sheets Below
    #
    def full_tear(self, factor: pd.Series, ntiles: int, holding_period: int, long_short: bool = True,
                  market_neutral=True, show_uni=False, show_ntile_tilts=False) -> Dict[str, BaseTear]:
        """
        Creates basic visualizations of the factor data distribution by ntile and how complete the data is
        Creates a fan chart of cumulative returns for the given factor values.
        Creates a IC time series for the factor value and the forward returns
        Createa a turnover sheet showing how often the factor data will turn over

        The in the cumulative return plot, each value represents the cumulative return up to that days close.
        Returns are not shifted each value represents portfolios value on the close of that day.

        A set of weights is generated for each day based off factor quantile.
        The portfolio is rebalanced daily, each days 1/holding_period of the portfolio is rebalanced.
        All positions are equally weighted.

        :param factor: The factor values being tested.
            index: (pd.Period, _asset_id)
            values: (factor_value)
        :param holding_period: How long we want to hold positions for, represents days
        :param ntiles: amount of bins we are testing (1 is high factor value n is low value)
        :param long_short: show we compute the spread between ntiles: (1 - n)
        :param market_neutral: subtract out the universe returns from the ntile returns?
        :return: plots showing the return profile of the factor
        :param show_uni: Should universe return be shown in the spread plot?
        :param show_ntile_tilts: should we show each ntiles tilts?
        """
        self._prep_for_run(factor, ntiles)
        tears = {'inspection_tear': InspectionTear(factor_data=self._factor_data),
                 'backtest_tear': TiltsBacktestTear(ntile_matrix=self._ntile_matrix,
                                                    daily_returns=self._formatted_returns, ntiles=ntiles,
                                                    holding_period=holding_period, long_short=long_short,
                                                    market_neutral=market_neutral,
                                                    show_uni=show_uni, factor_data=self._factor_data,
                                                    group_portal=self._group_portal,
                                                    show_ntile_tilts=show_ntile_tilts),
                 'ic_tear': ICTear(factor_data=self._factor_data, daily_returns=self._formatted_returns,
                                   holding_period=holding_period),
                 'turnover_tear': TurnoverTear(factor_data=self._factor_data, holding_period=holding_period)}
        self._run(tears)
        return tears

    def ntile_backtest_tear(self, factor: pd.Series, ntiles: int, holding_period: int, long_short: bool = True,
                            market_neutral=True, show_uni=False, show_ntile_tilts=False) -> Dict[str, BaseTear]:
        """
        Creates a fan chart of cumulative returns for the given factor values.
        The factor values are ntile'd into ntiles number of bins

        The in the cumulative return plot, each value represents the cumulative return up to that days close.
        Returns are not shifted each value represents portfolios value on the close of that day.

        A set of weights is generated for each day based off factor quantile.
        The portfolio is rebalanced daily, each days 1/holding_period of the portfolio is rebalanced.
        All positions are equally weighted.

        :param factor: The factor values being tested.
            index: (pd.Period, _asset_id)
            values: (factor_value)
        :param holding_period: How long we want to hold positions for, represents days
        :param ntiles: amount of bins we are testing (1 is high factor value n is low value)
        :param long_short: show we compute the spread between ntiles: (1 - n)
        :param market_neutral: subtract out the universe returns from the ntile returns?
        :return: plots showing the return profile of the factor
        :param show_uni: Should universe return be shown in the spread plot?
        :param show_ntile_tilts: should we show each ntiles tilts?
        """
        self._prep_for_run(factor, ntiles)
        tears = {'backtest_tear':
                     TiltsBacktestTear(ntile_matrix=self._ntile_matrix, daily_returns=self._formatted_returns,
                                       ntiles=ntiles, holding_period=holding_period, long_short=long_short,
                                       market_neutral=market_neutral, show_uni=show_uni, factor_data=self._factor_data,
                                       group_portal=self._group_portal, show_ntile_tilts=show_ntile_tilts)
                 }
        self._run(tears)
        return tears

    def ntile_inspection_tear(self, factor: pd.Series, ntiles: int) -> Dict[str, BaseTear]:
        """
        creates visuals showing the factor data over time
        only calculates IC for when the asset is in the universe
        :param factor: The factor values being tested.
            index: (pd.Period, _asset_id)
            values: (factor_value)
        :param ntiles: the number of ntiles
        :return: Dict of InspectionTear
        """
        self._prep_for_run(factor, ntiles)
        tears = {'inspection_tear': InspectionTear(factor_data=self._factor_data)}
        self._run(tears)
        return tears

    def ntile_ic_tear(self, factor: pd.Series, holding_period: int) -> Dict[str, BaseTear]:
        """
        creates visuals showing the ic over time
        :param factor: The factor values being tested.
            index: (pd.Period, _asset_id)
            values: (factor_value)
        :param holding_period: How long we want to hold positions for, represents days
        :return: Dict of ICTear
        """
        self._prep_for_run(factor, 1)
        tears = {'ic_tear': ICTear(factor_data=self._factor_data, daily_returns=self._formatted_returns,
                                   holding_period=holding_period)}
        self._run(tears)
        return tears

    def ntile_turnover_tear(self, factor: pd.Series, ntiles: int, holding_period: int) -> Dict[str, BaseTear]:
        """
        Creates visuals showing the turnover over time
        :param factor: The factor values being tested.
           index: (pd.Period, _asset_id)
           values: (factor_value)
        :param ntiles: the number of ntiles
        :param holding_period: How long we want to hold positions for, represents days
        :return: Dict of TurnoverTear
        """
        self._prep_for_run(factor, ntiles)
        tears = {'turnover_tear': TurnoverTear(factor_data=self._factor_data, holding_period=holding_period)}
        self._run(tears)
        return tears

    def ntile_ic_horizon(self, factor: pd.Series, intervals: Iterable[int], show_individual: bool = False) -> \
            Dict[str, BaseTear]:
        """
        Shows the curve of the information coefficient over various holding periods

        :param factor: The factor values being tested.
           index: (pd.Period, _asset_id)
           values: (factor_value)
        :param intervals: an iterable that contains the holding periods we would like to make the IC frontier for
        :param show_individual: should each individual IC time series be show for every interval
        :return: Dict of ICHorizonTear
        """
        self._prep_for_run(factor, 1)
        tears = {
            'ic_horizon_tear': ICHorizonTear(factor_data=self._factor_data, daily_returns=self._formatted_returns,
                                             intervals=intervals, show_individual=show_individual)}
        self._run(tears)
        return tears
