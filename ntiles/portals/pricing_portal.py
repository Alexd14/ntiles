from abc import ABC
from typing import Iterable, List, Union

import pandas as pd
import numpy as np
from .base_portal import BaseDeltaPortal

from toolbox import QueryConstructor


class PricingPortal(BaseDeltaPortal, ABC):
    """
    pulls pricing from database
    """

    def __init__(self, assets: Union[Iterable, str], search_by: str, start: str, end: str,
                 field: str = 'prc', schema: str = 'CRSP'):
        super().__init__(assets, pd.Period(start), pd.Period(end))
        self._search_by = search_by
        self._field = field
        self._schema = schema

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
        df = (QueryConstructor()
              .query_timeseries_table(self._schema + '.security_daily', assets=self._assets,
                                      start_date=str(self._start), end_date=str(self._end), search_by=self._search_by,
                                      fields=[self._field]).distinct()
              .set_calendar('NYSE')
              .order_by('date')
              .df)

        self._pricing = df[self._field].unstack().pct_change(1).iloc[1:]. \
            fillna(0).replace([np.inf, -np.inf], 0).clip(-.75, 1.5)
