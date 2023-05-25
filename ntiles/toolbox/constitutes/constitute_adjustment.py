from typing import List, Optional, Union

import duckdb
import pandas as pd

from ntiles.toolbox.utils.date_config import DateConfig
from ..db.read.query_constructor import QueryConstructor
from ..db.api.sql_connection import SQLConnection
from ..utils.handle_data import handle_duplicates

# this allows compatibility with python 3.6
try:
    import pandas_market_calendars as mcal
except ImportError as e:
    pass


class ConstituteAdjustment:
    """
    Provides the functionality of indexing a data to match a universe
    Correctly identifying on what day which asset should be in/not in the data set based on given universe data
    """

    def __init__(self,
                 id_col: str = 'permno',
                 date_config: DateConfig = None
                 ):
        """
        constructor for ConstituteAdjustment
        :param id_col: the asset identifier column for the data that will be passed
        :param date_type: should the date be outputted as a pd.Period or a pd.Timestamp?
        self._universe_factor: holds the index constitutes for the factor in a MultiIndex of date,
            self._id_col
        self._universe_pricing: holds the index constitutes for the pricing in a MultiIndex of date,
            self._id_col
        """
        self._id_col = id_col
        self._date_config = date_config

        self._universe_factor: Optional[pd.MultiIndex] = None

    def add_universe_info(self,
                          universe: pd.DataFrame,
                          start_date: str,
                          end_date: str,
                          calender: str = 'NYSE'
                          ) -> None:
        """
        Adds universe data to the ConstituteAdjustment object
        Creates a factors index which is simply the range of "from" to "thru"

        :param universe: a pandas data frame containing index component information.
                                MUST HAVE COLUMNS: self._id_col representing the asset identifier,
                                                   'from' start trading date on the index,
                                                   'thru' end trading date on the index,
                                If 'from', 'thru' are not pd.TimeStamps than a date_format MUST BE PASSED.
                                if no date_format is passed its assumed that they are in a pd.TimeStamp object
        :param start_date: The first date we want to get data for
        :param end_date: The last first date we want to get data for
        :param calender: The trading calender we want to use to get the dates
        :return: None
        """
        # making sure date and self._id_col are in the columns
        universe = _check_columns([self._id_col, 'from', 'thru'], universe)

        # will throw an error if there are duplicate self._id_col
        handle_duplicates(df=universe, out_type='ValueError', name=f'The column {self._id_col}',
                          drop=False, subset=[self._id_col])

        # making sure the dates are in the correct format
        universe = (self._date_config
                             .copy(target_data_type='timestamp', resample=False, grouper_keys=[])
                             .configure_dates(universe, ['from', 'thru']))

        relevant_cal = (mcal.get_calendar(calender)
                        .valid_days(start_date=start_date, end_date=end_date)
                        .to_frame(name='date'))
        relevant_cal = (self._date_config
        .copy(target_data_type='timestamp', resample=True, grouper_keys=[])
        .configure_dates(relevant_cal, 'date')
        .set_index('date')
        .rename({'index': 'date'}, axis=1)['date'])

        # making a list of series to eventually concat
        indexes_factor: List[pd.Series] = []

        for row in universe.iterrows():
            symbol = row[1][self._id_col]

            # getting the relevant dates for the factor
            date_range_factors: pd.Series = relevant_cal.loc[row[1]['from']: row[1]['thru']]

            # converting to frame and then stacking gives us a df with the index we are making, also speed improvement
            indexes_factor.append(
                date_range_factors.to_frame(symbol).stack()
            )

        # getting the index of the concatenated Series
        self._universe_factor = pd.concat(indexes_factor).index.set_names(['date', self._id_col])

    def add_universe_info_from_db(self,
                                  assets: str,
                                  start_date: str,
                                  end_date: str,
                                  sql_con=None
                                  ) -> None:
        """
        Same as add_universe_info but takes in universe info from the database,
        :param assets: The assets we want to get data for, Ex 'ETF_SPY'
        :param start_date: The first date we want to get data for string in %Y-%m-%d
        :param end_date: The last first date we want to get data for string in %Y-%m-%d
        :param sql_con: A connection to the sql database if not provided then will use default connection
        :return: None
        """
        over_con = sql_con is None
        if sql_con is None:
            sql_con = SQLConnection(':memory:', close_key=self.__class__.__name__)
        raw_uni = (QueryConstructor(sql_con=sql_con, cache=False, freq=None)
                   .query_universe_table(assets, fields=[self._id_col], start_date=start_date,
                                         end_date=end_date, override_sql_con=over_con)
                   .order_by('date')
                   .df)
        sql_con.close_with_key(self.__class__.__name__)
        self.add_universe_info_long(raw_uni, start_date, end_date)

        # raw_uni = (self._date_config
        #            .copy(target_data_type='timestamp')
        #            .configure_dates(raw_uni, 'date')
        #            .set_index(['date', self._id_col]))
        #
        # missing_id_for = raw_uni.index.to_frame()[self._id_col].isnull().sum() / len(raw_uni)
        # print(f"Universe missing \"{self._id_col}\" for {round(missing_id_for * 100, 2)}% of data points")
        #
        # self._universe_factor = raw_uni.index.dropna()

    def add_universe_info_long(self,
                               universe: pd.DataFrame,
                               start_date: Union[pd.Timestamp, str] = None,
                               end_date: Union[pd.Timestamp, str] = None
                               ) -> None:
        """
        Adds universe data to the ConstituteAdjustment object from a table with long format.
        :param universe: a pandas data frame containing universe component information.
        :param start_date: The first date we want to get data for
        :param end_date: The last first date we want to get data for
        :return: None
        """
        universe = _check_columns([self._id_col, 'date'], universe)[['date', self._id_col]]
        universe = (self._date_config
                             .copy(target_data_type='timestamp')
                             .configure_dates(universe, 'date'))
        universe = universe[(universe['date'] > start_date)
                                              & (universe['date'] < end_date)]
        self._universe_factor = universe.set_index(['date', self._id_col]).index

    def adjust_data_for_membership(self,
                                   data: pd.DataFrame,
                                   ) -> pd.DataFrame:
        """
        adjusts the data set accounting for when assets are a member of the index defined in add_universe_info.

        factor:
            Ex: AAPl joined S&P500 on 2012-01-01 and leaves 2015-01-01. GOOGL joined S&P500 on 2014-01-01 and is still
            in the index at the time of end_date passed in add_index_info. When passing data to the
            adjust_data_for_membership method it will only return AAPL factor data in range
            2012-01-01 to 2015-01-01 and google data in the range of 2014-01-01 to the end_date.

        :param data: A pandas dataframe to be filtered.
                    Must contain columns named self._id_col, 'date' otherwise can have as may columns as desired
        :param adjust_dates: If True then will adjust dates as depicted in date_config but will force timestamp output
        :return: An indexed data frame adjusted for when assets are in the universe
        """
        # if the add_index_info is not defined then throw error
        if self._universe_factor is None:
            raise ValueError('Universe is not set')

        # making sure date and self._id_col are in the columns
        data = _check_columns(['date', self._id_col], data, False)

        # if adjust_dates:
        data = (self._date_config
                .copy(resample=False, target_data_type='timestamp')
                .configure_dates(data, 'date'))

        # dropping duplicates and throwing a warning if there are any
        data = handle_duplicates(df=data, out_type='Warning', name='Data', drop=True, subset=['date', self._id_col])

        reindex_frame = self._fast_reindex(self._universe_factor, data)

        # if we have dataframe with 1 column then return series
        if reindex_frame.shape[1] == 1:
            return reindex_frame.iloc[:, 0]

        return reindex_frame

    def _fast_reindex(self,
                      reindex_by: pd.MultiIndex,
                      frame_to_reindex: pd.DataFrame
                      ) -> pd.DataFrame:
        """
        Quickly reindex a pandas dataframe using a join in duckdb
        :param reindex_by:Desired pandas Multiindex
        :param frame_to_reindex: Frame we are reindexing data from
        :return: Reindexed Dataframe
        """
        reindex_by = reindex_by.to_frame()

        id_cols = f'reindex_by.date, reindex_by.{self._id_col}'
        factor_cols = ', '.join([col for col in frame_to_reindex.columns if col not in ['date', self._id_col]])
        query = duckdb.query(f"""
                        SELECT {id_cols}, {factor_cols}
                            FROM reindex_by 
                                left join frame_to_reindex on (reindex_by.date = frame_to_reindex.date) 
                                                        and (reindex_by.{self._id_col} = frame_to_reindex.{self._id_col});
                        """)

        return self._set_dates(query.to_df()).set_index(['date', self._id_col])

    def _set_dates(self,
                   df: pd.DataFrame
                   ) -> pd.DataFrame:
        """
        adjusts the date column according to the self._date_type
        :param df: the Dataframe which we are adjusting the 'date column' for
        :return: df with date columns adjusted
        """
        return self._date_config.copy(resample=False).configure_dates(df, 'date')

    @property
    def factor_components(self) -> Optional[pd.MultiIndex]:
        """
        :return: Mutable list of tuples which represent the factor index constitutes
        """
        return self._universe_factor


def _check_columns(needed: List[str],
                   df: pd.DataFrame,
                   index_columns: bool = True
                   ) -> pd.DataFrame:
    """
    helper to check if the required columns are present
    raises value error if a col in needed is not in givenCols
    :param needed: list of needed columns
    :param df: df of the factor data for the given data
    :param index_columns: should we index the columns specified in needed when returning the df
    :return: Given dataframe with the correct columns and range index
    """
    if not isinstance(df.index, pd.core.indexes.range.RangeIndex):
        df = df.reset_index()

    for col in needed:
        if col not in df.columns:
            raise ValueError(f'Required column \"{col}\" is not present')

    if index_columns:
        return df[needed]

    return df
