from abc import ABC
from typing import List

import pandas as pd

try:
    from equity_db import MongoAPI, ReadDB
except ImportError:
    pass

from ntiles.portals.base_portal import BaseGrouperPortalConstant


class SectorPortal(BaseGrouperPortalConstant, ABC):
    def __init__(self, passed_assets: List[str], asset_id: str = 'lpermno', db: str = 'equity',
                 collection: str = 'crsp'):
        """
        :param asset_id: the assets to get the sector data for
        :param asset_id: what is the id of the asset, must be recognised by equity_db
        :param db: name of the db
        :param collection: name of the collection
        """
        super().__init__(passed_assets, 'GIC Sector')
        self._passed_assets = passed_assets
        self._asset_id = asset_id
        self._db = db
        self._collection = collection

        self._sectors = None
        self._set_sectors()

    @property
    def group_information(self) -> pd.Series:
        """
        gets the gic _sectors for the give assets
        :return: DataFrame of GIC _sectors for the given assets
        """
        if self._sectors is not None:
            return self._sectors

        self._set_sectors()
        return self._sectors

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
        reader = ReadDB(MongoAPI(db=self._db, collection=self._collection))
        query = reader.get_asset_data(self._passed_assets, search_by=self._asset_id, fields=['gsector'])
        self._sectors = query.df['gsector']
        self._sectors.index = self._sectors.index.astype(str)

    @property
    def assets(self) -> List[int]:
        return self._sectors.reset_index().lpermno.astype(int).tolist()
