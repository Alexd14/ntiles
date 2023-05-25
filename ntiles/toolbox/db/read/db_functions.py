import pandas as pd

from ntiles.toolbox.db.api.sql_connection import SQLConnection


def table_info(table_name: str, con=None) -> pd.DataFrame:
    """
    runs the table info PRAGMA query
    """
    con = con if con else SQLConnection(close_key='table_info')
    info_df = con.execute(f"PRAGMA table_info('{table_name}');").fetchdf()
    con.close_with_key('table_info')
    return info_df


def db_tables(con=None) -> pd.DataFrame:
    """
    runs PRAGMA query to get all table names
    """
    con = con if con else SQLConnection(close_key='db_tables')
    info_df = con.execute(f"PRAGMA show_tables;").fetchdf()
    con.close_with_key('db_tables')
    return info_df
