from abc import ABC
from typing import Iterable, List, Union

import pandas as pd

from toolbox import QueryConstructor
from .base_portal import BaseGrouperPortalConstant


class SectorPortal(BaseGrouperPortalConstant, ABC):
    def __init__(self, assets: Union[Iterable, str], search_by: str = 'permno', field='gsector', con=None,
                 start_date=None, end_date=None, ):
        """
        :param assets: the assets or universe to get the sector data for
        :param search_by: what is the id of the asset
        :param field: name of field we want to get
        """
        super().__init__(assets, 'GIC Sector')
        self._search_by = search_by
        self._field = field
        self._con = con
        self._start_date = start_date
        self._end_date = end_date

        self._group = None
        self._set_sectors()

    @property
    def group_information(self) -> pd.Series:
        """
        gets the gic _sectors for the give assets
        :return: DataFrame of GIC _sectors for the given assets
        """
        return self._group

    @property
    def group_mapping(self):
        """
        :return: dict mapping for the group
        """
        return self.group_information.to_dict()

    def _set_sectors(self) -> None:
        """
        Sets the _sectors in the class
        :return: None
        """
        self._group = (QueryConstructor(self._con)
                       .query_no_date_table(table='link.crsp_cstat_link', fields=[self._field, 'lpermno as permno'],
                                            assets=self._assets, search_by=self._search_by, start_date=self._start_date,
                                            end_date=self._end_date)
                       .df)[self._field].fillna(-1)

    @property
    def assets(self) -> List[int]:
        return self._group.index.tolist()
