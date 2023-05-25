import glob
import os.path

import pandas as pd

from typing import Union

from ntiles.toolbox.db.settings import ADD_ALL_LINKS_TO_PERMNO, ETF_UNI_DIRECTORY, BUILT_UNI_DIRECTORY
from ntiles.toolbox.db.api.sql_connection import SQLConnection

# this allows compatibility with python 3.6
try:
    import pandas_market_calendars as mcal
except ImportError as e:
    pass

MAP_ETF_SYMBOL_ID = {'SPY': 1021980,
                     'IWM': 1025818,
                     'IWV': 1025817}


def dispatch_universe_path(uni_name, add_quotes=False, sql_con=None) -> str:
    """
    gets the path to the given universe.
    Can pass any universe and will figure out the correct path to the universe.
    Assumes that the universe name is valid.

    :param uni_name: the name of the universe
    :param sql_con: a connection to the database
    :param add_quotes: should we add single quotes around the path?
    :return: path to the universe
    """
    #  user passes a etf to use as universe
    if 'ETF' in uni_name:
        out = ETFUniverse(con=sql_con).get_universe_path_parse(uni_name)

    # user passes a built universe
    else:
        out = BuiltUniverse().get_universe_path(uni_name)

    if add_quotes:
        return f"'{out}'"

    return out


class ETFUniverse:
    """
    CLass to build universes from etf holdings.
    Will cache the universes in parquet files to be read into duckdb instances
    """

    def __init__(self, con: SQLConnection = None):
        """
        If the user would like to class this class mutable times then they must pass a connection to con
        :param con: A connection to the database, if not passed then will open a new connection.
        """
        self._con = con if con else SQLConnection(close_key=self.__class__.__name__)

    def get_universe_df(self, ticker: str = None, crsp_portno: int = None, start_date: str = '2000',
                        end_date: str = '2023') -> Union[pd.DataFrame, str]:
        """
        gets the universe constitutes for an etf
        either ticker or crsp_portno must be passed but not both
        :param ticker: the ticker of the etf we are getting holdings for
        :param crsp_portno: the crsp_portno of the etf we are getting holdings for
        :param start_date: the date to start getting holdings for YYYY-MM-DD, no effect when caching
        :param end_date: the date to stop getting holdings YYYY-MM-DD, no effect when caching
        :return: pd.Dataframe index: int_range; columns: date, permno;
        """

        self._input_checks(ticker=ticker, crsp_portno=crsp_portno)

        asset_id = self._get_crsp_portno(ticker=ticker, crsp_portno=crsp_portno)

        if not self._is_cached_etf(crsp_portno=asset_id):
            etf_uni = self._cache_etf(crsp_portno=asset_id)

        else:
            etf_uni = self._get_cached_etf(crsp_portno=asset_id)

        return etf_uni[(etf_uni['date'] > start_date) & (etf_uni['date'] < end_date)]

    def get_universe_path(self, ticker: str = None, crsp_portno: int = None):
        """
        gets the SQL code to read cached universe constitutes for an etf
        either ticker or crsp_portno must be passed but not both
        if etf isn't cached then will cache the etf
        :param ticker: the ticker of the etf we are getting holdings for
        :param crsp_portno: the crsp_portno of the etf we are getting holdings for
        :return: SQL code to read the cached parquet
        """
        self._input_checks(ticker=ticker, crsp_portno=crsp_portno)

        asset_id = self._get_crsp_portno(ticker=ticker, crsp_portno=crsp_portno)

        if not self._is_cached_etf(crsp_portno=asset_id):
            self._cache_etf(crsp_portno=asset_id)

        return self._get_cached_path(asset_id)

    def get_universe_path_parse(self, to_parse):
        """
        wrapper that parses a string for get_universe_path, can tell if user passed a symbol or crsp_portno
        format:
            ticker:
                'ETF_SPY'
            crsp_portno:
                'ETF_5648362'
        """
        to_parse = to_parse.upper()
        param_dict = self._parse_etf_uni_string(to_parse, param_dict={})
        return self.get_universe_path(**param_dict)

    def get_universe_df_parse(self, to_parse, start_date: str = '2000', end_date: str = '2023'):
        """
        wrapper that parses a string for get_universe_path, can tell if user passed a symbol or crsp_portno
        format:
            ticker:
                'ETF_SPY'
            crsp_portno:
                'ETF_5648362'
        """
        param_dict = {'start_date': start_date, 'end_date': end_date}
        param_dict = self._parse_etf_uni_string(to_parse, param_dict=param_dict)
        return self.get_universe_df(**param_dict)

    def _cache_etf(self, crsp_portno) -> pd.DataFrame:
        """
        gets and caches an etf holdings query
        will cache etf in temp directory of the computer
        :return: pd.Dataframe index: int_range; columns: date, permno;
        """
        print('Caching ETF Holdings')

        sql_for_holdings = f"""
                SELECT DISTINCT date, permno 
                FROM crsp.portfolio_holdings
                WHERE crsp_portno = {crsp_portno} AND 
                    permno IS NOT NULL
               """
        raw_etf_holdings = self._con.execute(sql_for_holdings).fetchdf()
        self._con.close_with_key(close_key=self.__class__.__name__)

        df_of_holdings = raw_etf_holdings.set_index('date').groupby('date')['permno'].apply(
            lambda grp: list(grp.value_counts().index))

        end_date = pd.Timestamp.now().date().strftime('%Y-%m-%d')
        start_date = df_of_holdings.index.min()

        full_cal = pd.date_range(start=start_date, end=end_date, freq='D').tz_localize(None)

        trading_cal = mcal.get_calendar(
            'NYSE').valid_days(start_date=start_date, end_date=end_date).tz_localize(
            None)

        universe = df_of_holdings.reindex(full_cal.tolist()).ffill().reindex(trading_cal.tolist()).reset_index()

        relational_format = [(row[0], permno) for row in universe.values for permno in row[1]]
        uni_df = pd.DataFrame(relational_format, columns=['date', 'permno'])
        uni_df = self._link_to_ids(uni_df)

        self._cache_helper(uni_df=uni_df, crsp_portno=crsp_portno)

        return uni_df

    def _link_to_ids(self, uni_df: pd.DataFrame) -> pd.DataFrame:
        """
        join cstat and ibes links to current universe df
        """
        columns = ', '.join(['date', 'uni.permno', 'lpermco as permco', 'gvkey', 'liid as iid', 'ticker', 'cusip',
                             "CASE WHEN gvkey NOT NULL THEN CONCAT(gvkey, '_', liid) ELSE NULL END as id"])
        from_start = " uni_df as uni "

        sql_code = ADD_ALL_LINKS_TO_PERMNO.replace('--columns', columns).replace('--from', from_start)

        return self._con.con.execute(sql_code).fetchdf()

    def _get_crsp_portno(self, ticker, crsp_portno) -> int:
        """
        if ticker is not none then will map the ticker to a crsp_portno
        :return: crsp_portno passed or the crsp_portno mapped to the symbol
        """
        if crsp_portno:
            return crsp_portno

        mapped_id = self._con.execute(f"""SELECT distinct crsp_portno 
                                         FROM crsp.fund_summary 
                                         WHERE ticker = '{ticker}' AND
                                            crsp_portno IS NOT NULL""").fetchall()

        if len(mapped_id) == 0:
            self._con.close_with_key(close_key=self.__class__.__name__)
            raise ValueError(f"Ticker '{ticker}' is not valid cant map to crsp_portno")

        if len({x[0] for x in mapped_id}) > 1:
            # getting metadata of the portno's that mtched
            mapped_funds = self._con.execute(f"""SELECT DISTINCT crsp_portno, fund_name, m_fund, et_flag
                                            FROM crsp.fund_summary 
                                            WHERE ticker = '{ticker}' AND
                                                crsp_portno IS NOT NULL""").fetchdf()
            self._con.close_with_key(close_key=self.__class__.__name__)

            raise ValueError(f"Ticker '{ticker}' mapped to {len(mapped_id)} crsp_portno's {mapped_id}. "
                             f"Please specify the crsp_portno to build this etf's history\n" +
                             mapped_funds.to_string(index=False))

        return int(mapped_id[0][0])

    @staticmethod
    def _input_checks(ticker, crsp_portno) -> None:
        """
        input check for ticker and crsp_portno
        """
        if ticker is None and crsp_portno is None:
            raise ValueError('Must pass a ticker or crsp_portno')

        if ticker is not None and crsp_portno is not None:
            raise ValueError('Must pass a ticker or crsp_portno, not both!')

    def _is_cached_etf(self, crsp_portno) -> bool:
        """
        is the etf cached?
        """
        return os.path.isfile(self._get_cached_path(crsp_portno))

    def _cache_helper(self, uni_df, crsp_portno) -> None:
        """
        Writes a parquet file to the user specified temp directory on a computer
        """
        path = self._get_cached_path(crsp_portno)
        uni_df.to_parquet(path)
        print(f'Cached {crsp_portno} in {path}')

    def _get_cached_etf(self, crsp_portno) -> pd.DataFrame:
        """
        returns a dataframe of the cached universe
        """
        return pd.read_parquet(self._get_cached_path(crsp_portno))

    @staticmethod
    def _get_cached_path(crsp_portno):
        """
        :return: path to the cached file
        """
        return f'{ETF_UNI_DIRECTORY}/etf_uni_{crsp_portno}.parquet'

    @staticmethod
    def _parse_etf_uni_string(to_parse: str, param_dict: dict) -> dict:
        """
        adds 'ticker' or 'crsp_portno' a parameter dict
        :params to_parse:
        :params param_dict: dict which we will add 'ticker' or 'crsp_portno' to
        :return: param_dict with 'ticker' or 'crsp_portno' added
        """
        to_parse = to_parse.upper()
        if 'ETF_' in to_parse:
            id_etf = to_parse.split('_')[-1]
            if id_etf.isdigit():
                param_dict['crsp_portno'] = to_parse.split('_')[-1]
            else:
                param_dict['ticker'] = to_parse.split('_')[-1]
        else:
            raise ValueError(f"Can't parse {to_parse}")

        return param_dict


class BuiltUniverse:
    """
    Gets and validates path to a built universe
    """

    def get_universe_path(self, uni_name) -> str:
        """
        gets the path to the parquet file of the given universe
        :param uni_name: the name of the universe ex: CRSP_US_1000
        :return: the full path to the given universe
        :raises: ValueError if given uni_name is invalid
        """
        self._ensure_universe_exists(uni_name)
        return self._get_path(uni_name)

    def _ensure_universe_exists(self, uni_name):
        """
        checks to see if the universe exisis
        """
        if not os.path.isfile(self._get_path(uni_name)):
            raise ValueError(f'Universe {uni_name} does not exist!')

    @staticmethod
    def _get_path(uni_name):
        """
        creates what the path should be to the universe file
        """
        return f'{BUILT_UNI_DIRECTORY}/{uni_name.upper()}.parquet'


def clear_etf_universes():
    """
    Clears all parquet files in the ETF_UNI_DIRECTORY path
    """
    files = glob.glob(f'{ETF_UNI_DIRECTORY}/*.parquet')
    for f in files:
        os.remove(f)
    print('Cleared ETF Universes')


def clear_built_universes():
    """
    Clears all parquet files in the BUILT_UNI_DIRECTORY path
    """
    files = glob.glob(f'{BUILT_UNI_DIRECTORY}/*.parquet')
    for f in files:
        os.remove(f)
    print('Cleared Built Universes')


if __name__ == '__main__':
    print(ETFUniverse().get_universe_df_parse(to_parse='ETF_1021980', start_date='2017'))
