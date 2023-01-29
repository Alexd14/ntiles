from abc import ABC
from typing import Iterable, Union

import pandas as pd
import numpy as np
from .base_portal import BaseDeltaPortal

from toolbox import ConstituteAdjustment, QueryConstructor, SQLConnection


class PricingPortal(BaseDeltaPortal, ABC):
    """
    pulls pricing from database
    """

    def __init__(self, assets: Union[Iterable, str], search_by: str, start_date: str, end_date: str,
                 field: str = 'prc', table: str = 'CRSP.sd', con: SQLConnection = None):
        super().__init__(assets, pd.Period(start_date),
                         min(pd.Timestamp(end_date), pd.Timestamp('today')).to_period('D'))
        self._search_by = search_by
        self._field = field
        self._table = table
        self._con = con

        self._pricing = None
        self._get_pricing()

    @property
    def assets(self):
        return self._pricing.columns.tolist()

    @property
    def delta_data(self):
        """
        returns the delta of the data held by the portal
        :return: Index: Id, pd.Period; Columns: 'delta'; Values: data
        """
        return self._pricing

    @property
    def periods(self):
        return self._pricing.index.drop_duplicates().to_list()

    def _get_pricing(self):
        df = (QueryConstructor(sql_con=self._con)
              .query_timeseries_table(self._table, assets=self._assets,
                                      start_date=str(self._start), end_date=str(self._end), search_by=self._search_by,
                                      fields=[self._field])
              .distinct()
              .set_calendar('NYSE')
              .order_by('date')
              .dropna(self._field)
              .df)

        self._pricing = df[self._field].unstack().pct_change(1).iloc[1:]. \
            fillna(0).replace([np.inf, -np.inf], 0).clip(-.75, 1.5)
