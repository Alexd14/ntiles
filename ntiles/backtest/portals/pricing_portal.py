from abc import ABC
from typing import Iterable, List, Union

import pandas as pd
import numpy as np
from .base_portal import BaseDeltaPortal

from ntiles.toolbox import QueryConstructor, SQLConnection


class PricingPortal(BaseDeltaPortal, ABC):
    """
    Pulls pricing from database
    """

    def __init__(self,
                 assets: Union[Iterable, str],
                 search_by: str,
                 start_date: str,
                 end_date: str,
                 field: str = 'prc',
                 table: str = 'CRSP.sd',
                 con: SQLConnection = None,
                 freq: str = 'D',
                 ):
        """
        :param assets: The assets we want ti search for. Can be list of ids or a code eg "ETF_SPY".
        :param search_by: The name of the asset ids we are searching the database by.
        :param start_date: The date to start getting pricing. Format: %Y-%m-%d
        :param end_date: The date to stop getting pricing. Format: %Y-%m-%d
        :param field: The pricing field to get from the database. Default: 'prc'
        :param table: The table to get the pricing from. Default: 'CRSP.sd'
        :param con: A SQLConnection object to use to connect to the database. Default: None
        :param freq: The frequency of the pricing. Default: 'D'
        """
        super().__init__(assets=assets,
                         start=pd.Period(start_date),
                         end=min(pd.Timestamp(end_date), pd.Timestamp('today')).to_period('D'),
                         freq=freq)
        self._search_by = search_by
        self._field = field
        self._table = table
        self._con = con
        self._freq = freq

        self._pricing = None
        self._get_pricing()

    @property
    def assets(self) -> List[any]:
        return self._pricing.columns.tolist()

    @property
    def delta_data(self) -> pd.DataFrame:
        """
        returns the delta of the data held by the portal
        :return: Index: Id, pd.Period; Columns: 'delta'; Values: data
        """
        return self._pricing

    @property
    def periods(self) -> List[pd.Period]:
        return self._pricing.index.drop_duplicates().to_list()

    def _get_pricing(self):
        df = (QueryConstructor(sql_con=self._con, freq=self._freq)
              .query_timeseries_table(self._table, assets=self._assets,
                                      start_date=str(self._start), end_date=str(self._end),
                                      search_by=self._search_by, fields=[self._field])
              .distinct()
              .set_calendar('NYSE')
              .order_by('date')
              .dropna(self._field)
              .df)

        self._pricing = df[self._field].unstack().pct_change(1).iloc[1:]. \
            fillna(0).replace([np.inf, -np.inf], 0).clip(-.75, 1.5)
