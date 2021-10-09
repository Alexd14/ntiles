from abc import ABC, abstractmethod
from typing import List, Union

import pandas as pd


class BasePortal(ABC):
    """
    Base class for the data portal object
    """

    def __init__(self, assets: List[Union[str, int]]):
        """
        :param assets: the assets we are querying for
        """
        self._assets = assets

    @property
    @abstractmethod
    def assets(self) -> List[Union[str, int]]:
        """
        returns the assets property
        """
        return self._assets


class BaseTimeSeriesPortal(BasePortal):
    """portal for time series data"""

    def __init__(self, assets: List[Union[str, int]], start: pd.Period, end: pd.Period):
        """
        :param assets: the assets we are querying for
        :param start: start date for the query
        :param end: end date for the query
        """
        super().__init__(assets)
        self._start = start
        self._end = end

    @property
    @abstractmethod
    def periods(self) -> List[pd.Period]:
        """
        :return: the unique periods for which we have data
        """
        pass


class BaseRawPortal(BaseTimeSeriesPortal, ABC):
    def __init__(self, assets: List[Union[str, int]], start: pd.Period, end: pd.Period):
        """
        :param assets: the assets we are querying for
        :param start: start date for the query
        :param end: end date for the query
        """
        super().__init__(assets, start, end)

    @property
    @abstractmethod
    def raw_data(self) -> pd.DataFrame:
        """
        returns the raw data held by the portal
        :return: Index: Id, pd.Period; Columns: 'data'; Values: data
        """
        pass


class BaseDeltaPortal(BaseTimeSeriesPortal, ABC):
    """
    a portal which fetches and calculates the raw data long with delta or percent delta of a variable.
    Useful for fetching and calculating returns
    """

    @property
    @abstractmethod
    def delta_data(self):
        """
        returns the delta of the data held by the portal
        :return: Index: Id, pd.Period; Columns: 'delta'; Values: data
        """
        pass


class BaseGrouperPortalConstant(BasePortal, ABC):
    """
    A portal which fetches grouping data
    """

    def __init__(self, assets: List[Union[str, int]], group_name: str):
        """
        :param assets: the assets we are querying for
        :param group_name: the name of the grouping
        """
        super().__init__(assets)
        self.group_name = group_name

    @property
    def name(self):
        """
        :return: Name of group
        """
        return self.group_name

    @property
    @abstractmethod
    def group_information(self) -> pd.Series:
        """
        Holds group information from the portal
        :return: Index: Id; Columns: 'group'; Values: group
        """
        pass

    @property
    @abstractmethod
    def group_mapping(self):
        """
        :return: dict mapping for the group
        """
        pass


class BaseGrouperPortalTimeSeries(BaseTimeSeriesPortal, ABC):
    """
    a portal which returns grouping information over a time period
    """

    def __init__(self, assets: List[Union[str, int]], start: pd.Period, end: pd.Period, group_name: str):
        """
        :param assets: the assets we are querying for
        :param start: start date for the query
        :param end: end date for the query
        :param group_name: the name of the grouping
        """
        super().__init__(assets, start, end)
        self.group_name = group_name

    @property
    def name(self):
        """
        :return: Name of group
        """
        return self.group_name

    @property
    @abstractmethod
    def periods(self) -> List[pd.Period]:
        """
        :return: the unique periods for which we have data
        """
        pass

    @property
    @abstractmethod
    def group_information(self) -> pd.DataFrame:
        """
        Holds a timeseries of group information from the portal
        :return: Index: Id, pd.Period; Columns: 'group'; Values: group
        """
        pass
