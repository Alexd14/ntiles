from abc import ABC

import pandas as pd
from typing import Optional, List
from equity_db import MongoAPI, ReadDB

from ntiles.portals.base_portal import BaseDeltaPortal


class PricingPortal(BaseDeltaPortal, ABC):
    """
    Object to query and cache pricing data
    """

    def __init__(self, assets: List[str], start: pd.Timestamp, end: pd.Timestamp, pricing_field: str = 'prccd',
                 adjustor_field: str = 'ajexdi', db: str = 'equity', collection: str = 'crsp',
                 trading_calender='NYSE'):
        """
        :param assets: The assets to get data for
        :param start: start of period to query
        :param end: end of period to query
        :param pricing_field: what field to use for pricing data
        :param adjustor_field: What field to use for adjusting the pricing data
        :param db: the data base to use
        :param collection: the collection to query
        :param trading_calender: the trading calendar to use to verify dates
        """
        super().__init__(assets, start.to_period('D'), end.to_period('D'))

        self._pricing_field = pricing_field

        self._adjusted_pricing: Optional[pd.DataFrame] = None
        self._period_delta: Optional[pd.DataFrame] = None
        self._query_adjusted_pricing(db, collection, assets, start, end, pricing_field, adjustor_field,
                                     trading_calender)

    @property
    def delta_data(self):
        """
        :return: unstacked daily asset returns
            col: asset_id; index: pd.period; values: daily asset returns
        """
        if self._period_delta is None:
            self._period_delta = self.raw_data.unstack().pct_change(1).iloc[1:].fillna(0)

        return self._period_delta

    @property
    def raw_data(self) -> pd.Series:
        """
        adjustments to AssetQuery:
            1) Turns date column from pd.Timestamp into pd.Period
            2) Turns the lpermno into an int
            3) Adjusts the pricing_field: pricing_field / adjustor_field
        :return: Series of the adjusted pricing data indexed by date, lpermno
        """
        if self._adjusted_pricing is None:
            raise ValueError('adjusted pricing is not set')

        return self._adjusted_pricing[self._pricing_field]

    @property
    def assets(self) -> List[int]:
        """
        casting to int due to db problem must fix
        :return: The id's of assets we have pricing data for
        """
        return self._adjusted_pricing.index.get_level_values('id').astype(int).unique().tolist()

    @property
    def periods(self) -> List[pd.Period]:
        """
        :return: the unique periods for which we have pricing data
        """
        return self._adjusted_pricing.index.get_level_values('date').unique().tolist()

    def _query_adjusted_pricing(self, db, collection, assets, start, end, pricing_field, adjustor_field,
                                trading_calender) -> None:
        """
        Makes query the pricing data
        Performs adjustments defined in self.daily_pricing
        Then caches the adjusted pricing in self._adjusted_pricing
        self._adjusted_pricing columns: self._pricing_field, self._adjustor_field, Index: date, lpermno
        :return: None, mutates self._adjusted_pricing to contain adjusted pricing
        """
        # querying pricing
        reader = ReadDB(MongoAPI(db, collection))
        query_df = reader.get_asset_data(assets, search_by='lpermno', start=start, end=end,
                                         fields=[pricing_field, adjustor_field])
        pricing_df = query_df.set_calendar(trading_calender).df.reset_index()

        # possibly send command to close mongo to free up memory

        # adjusting data frame
        pricing_df[pricing_field] = pricing_df[pricing_field] / pricing_df[adjustor_field]
        pricing_df['date'] = pricing_df['date'].dt.to_period(freq='D')

        # currently code is requiring lpermno input wont work with tickers need to fix db
        pricing_df['id'] = pricing_df['lpermno'].astype(int)
        pricing_df = pricing_df.set_index(['date', 'id'])

        self._adjusted_pricing = pricing_df

        self._query_summary(assets)  # this can be cleaner

    def _query_summary(self, assets):
        """
        prints a summary of query and tells you what id's were not able to be found in the query
        :return: None
        """
        query_assets = self._adjusted_pricing.index.get_level_values('date')
        not_found_assets = set(assets) - set(query_assets)
        if len(not_found_assets) == 0:
            print('All assets retrieved in query!')
        else:
            print(f'Unable to find {len(not_found_assets)} assets: {not_found_assets}')
