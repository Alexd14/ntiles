import glob
import hashlib
import os

import pandas as pd

from datetime import datetime

from ntiles.toolbox.db.settings import CACHE_DIRECTORY
from ntiles.toolbox.db.api.sql_connection import SQLConnection


class CachedQuery:
    """
    Functionality to cache results of a QueryConstructor
    """

    def __init__(self, query: str):
        """
        :param query: the query we are looking at
        """
        self._query = query
        self._query_hash = hashlib.sha224(query.encode()).hexdigest()
        # what the path should be to the cache file
        self._path = f'{CACHE_DIRECTORY}/{self._query_hash.upper()}.parquet'

    def is_query_cached(self) -> bool:
        """
        checks to see if the query is cached
        """
        return os.path.isfile(self._path)

    def cache_query(self, results: pd.DataFrame):
        """
        caches the given results
        If index is not range index then will write index as a column not an index
        """
        if not isinstance(results.index, pd.RangeIndex):
            results = results.reset_index()

        # if any columns are a period type change them to timestamp
        con = SQLConnection(':memory:')
        con.execute(f"COPY results TO '{self._path}' (FORMAT 'parquet')")
        con.close()
        print(f'Cached Query')

    def get_cached_query_path(self) -> str:
        """
        gets the path to the cached query will rase ValueError if the query is not cached
        """
        if self.is_query_cached():
            return self._path
        raise ValueError('Query is not cached!')

    def get_cached_query_df(self) -> pd.DataFrame:
        """
        gets the DataFrame contents of the cached query will rase ValueError if the query is not cached
        The index will be a default range index
        """
        path = f"'{self.get_cached_query_path()}'"

        con = SQLConnection(':memory:')
        cached_results = con.execute(f"SELECT * FROM {path}").df()
        con.close()

        file_creation = datetime.fromtimestamp(os.stat(self._path).st_birthtime)
        file_age = (datetime.now() - file_creation).days

        print(f'Using {file_age} Day Old Cache')
        return cached_results


def clear_cache():
    files = glob.glob(f'{CACHE_DIRECTORY}/*.parquet')
    for f in files:
        os.remove(f)
    print('Cleared Cache')
