from abc import ABC
from typing import List

import pandas as pd
from equity_db import MongoAPI, ReadDB

from ntiles.portals.base_portal import BaseGrouperPortalConstant


class SectorPortal(BaseGrouperPortalConstant, ABC):
    def __init__(self, passed_assets: List[str], asset_id='lpermno', db: str = 'equity', collection: str = 'crsp'):
        """
        :param assets: the assets to get the sector data for must be lpermno
        :param asset_id: what is the id of the asset, must be recognised by equity_db
        :param db: name of the db
        :param collection: name of the collection
        """
        super().__init__(passed_assets, 'GIC Sector')
        self.passed_assets = passed_assets
        self.asset_id = asset_id
        self.db = db
        self.collection = collection

        self.sectors = None
        self.set_sectors()

    @property
    def group_information(self) -> pd.Series:
        """
        gets the gic sectors for the give assets
        :return: DataFrame of GIC sectors for the given assets
        """
        if self.sectors is not None:
            return self.sectors

        self.set_sectors()
        return self.sectors

    @property
    def group_mapping(self):
        """
        :return: dict mapping for the group
        """
        return self.group_information.to_dict()

    def set_sectors(self) -> None:
        """
        Sets the sectors in the class
        :return: None
        """
        reader = ReadDB(MongoAPI(db=self.db, collection=self.collection))
        query = reader.get_asset_data(self.passed_assets, search_by=self.asset_id, fields=['gsector'])
        self.sectors = query.df['gsector']
        self.sectors.index = self.sectors.index.astype(str)


    @property
    def assets(self) -> List[int]:
        return self.sectors.reset_index().lpermno.astype(int).tolist()