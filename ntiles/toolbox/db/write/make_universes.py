import logging

import pandas as pd

from ntiles.toolbox.db.api.sql_connection import SQLConnection
from ntiles.toolbox.db.settings import ADD_ALL_LINKS_TO_PERMNO, BUILT_UNI_DIRECTORY

# this allows compatibility with python 3.6
try:
    import pandas_market_calendars as mcal
except ImportError as e:
    pass

logging.basicConfig(format='%(message)s ::: %(asctime)s', datefmt='%I:%M:%S %p', level=logging.INFO)


def compustat_us_universe(max_rank: int, min_rank: int = 1, start_date: str = '2000',
                          rebuild_mc_ranking: bool = False) -> None:
    """
    generates US daily indexes for compustat daily security file
    only will use the primary share for a company
    will generate a table called universe.US_min_rank_max_rank, ex US_0_3000
    :param max_rank: the max market cap rank for a company to be in the universe
    :param min_rank: the min market cap rank for a company in the universe
    :param start_date: the minimum date for creating the universe
    :param set_indexes: Should we index the universe by
    :return: None
    """

    table_name = f'CSTAT_US{"" if min_rank == 1 else "_" + str(min_rank)}_{max_rank}'
    write_path = f'{BUILT_UNI_DIRECTORY}/{table_name}.parquet'

    if rebuild_mc_ranking:
        _make_cstat_us_universe_base_table()
    else:
        logging.info(f'Using Prior Build of universe.cstat_mc_rank')

    logging.info(f'Creating table {table_name}')

    sql_make_universe_table = f""" 
        COPY 
            (
            SELECT date, gvkey, iid, id, ttm_min_prccd, ttm_mc, ttm_mc_rank
            FROM universe.temp_rank_cstat_mc
            WHERE ttm_mc_rank >= {min_rank} AND 
                ttm_mc_rank <= {max_rank} AND 
                date > '{start_date}'
            )
        TO '{BUILT_UNI_DIRECTORY}/{table_name}.parquet' (FORMAT 'parquet')
        """
    # making the db connection
    con = SQLConnection(read_only=False).con

    con.execute(sql_make_universe_table)
    con.close()

    logging.info(f'Wrote Table {table_name} To {write_path}')


def crsp_us_universe(max_rank: int, min_rank: int = 1, start_date: str = '1980',
                     rebuild_mc_ranking: bool = False, link: bool = True) -> None:
    """
    Generates a universe of the top N stocks domiciled in the US by market cap
    Will only use companies primary share
    :param max_rank: the max market cap rank for a company to be in the universe
    :param min_rank: the min market cap rank for a company in the universe
    :param start_date: the minimum date for creating the universe
    :param set_indexes: Should we index the universe by
    :param rebuild_mc_ranking: should we rebuild the ranking table universe.crsp_mc_rank?
    :param link: should we link to cstat and ibes
    :return: None
    """
    # getting the trading calendar so we dont have bad dates
    trading_cal = mcal.get_calendar(
        'NYSE').valid_days(start_date=start_date, end_date=pd.to_datetime('today')).to_series().to_frame('trading_days')

    table_name = f'CRSP_US{"" if min_rank == 1 else "_" + str(min_rank)}_{max_rank}'
    write_path = f'{BUILT_UNI_DIRECTORY}/{table_name}.parquet'

    if rebuild_mc_ranking:
        _make_crsp_us_universe_base_table()
    else:
        logging.info(f'Using Prior Build of universe.crsp_mc_rank')

    logging.info(f'Creating table {table_name}')

    sql_make_universe_table = f""" 
        (
        SELECT date, permno, permco, ttm_min_prc, ttm_mc, ttm_mc_rank
        FROM universe.temp_rank_crsp_mc 
        WHERE ttm_mc_rank >= {min_rank} AND 
            ttm_mc_rank <= {max_rank} AND 
            date > '{start_date}'
        ) as uni
        """

    # will add linking tables
    if link:
        columns = ', '.join(['uni.*', 'gvkey', 'liid as iid, ''ticker', 'cusip',
                             "CASE WHEN gvkey NOT NULL THEN CONCAT(gvkey, '_', liid) ELSE NULL END as id"])
        sql_make_universe_table = '(' + (ADD_ALL_LINKS_TO_PERMNO
                                         .replace('--columns', columns)
                                         .replace('--from', sql_make_universe_table)) + ')'

    sql_make_universe_table = f"""COPY 
                                    {sql_make_universe_table} 
                                    TO '{write_path}' (FORMAT 'parquet')"""

    # making the db connection
    con = SQLConnection(read_only=False).con
    con.execute(sql_make_universe_table)
    con.close()

    logging.info(f'Wrote Table {table_name} To {write_path}')


def _make_cstat_us_universe_base_table():
    """
    Makes the base table with market cap ranks for each asset. Should be deleted after its done being used
    """
    table_name = 'universe.temp_rank_cstat_mc'
    logging.info(f'Creating Ranking Table {table_name}')

    # getting the trading calendar so we dont have bad dates
    trading_cal = mcal.get_calendar(
        'NYSE').valid_days(start_date='1980', end_date=pd.to_datetime('today')).to_series().to_frame('trading_days')

    sql_ensure_schema_open = f'CREATE SCHEMA IF NOT EXISTS universe;'
    sql_ensure_table_open = f'DROP TABLE IF EXISTS {table_name};'
    sql_make_rank_universe_table = f""" 
            CREATE TABLE {table_name}
            AS
                (
                SELECT date, gvkey, iid, id, ttm_min_prccd, ttm_mc, 
                    row_number() OVER (PARTITION BY (date) ORDER BY ttm_mc desc) AS ttm_mc_rank
                FROM
                    (
                    SELECT * 
                    FROM
                        (
                        SELECT date, gvkey, iid, id, 
                            AVG(ABS(prccd) * cshoc) OVER (
                            PARTITION BY id ORDER BY date ROWS BETWEEN 252 PRECEDING AND CURRENT ROW) AS ttm_mc,
                            MIN(ABS(prccd)) OVER (
                            PARTITION BY id ORDER BY date ROWS BETWEEN 252 PRECEDING AND CURRENT ROW) AS ttm_min_prccd
                        FROM 
                            (
                            SELECT date, gvkey, iid, id, priusa, fic, tpci, curcdd,
                                lag(prccd, 1, NULL) OVER lagDays AS prccd, 
                                lag(cshoc, 1, NULL) OVER lagDays AS cshoc
                            FROM main.sd AS sd RIGHT JOIN trading_cal cal ON sd.date = cal.trading_days 
                            WINDOW lagDays AS (PARTITION BY id ORDER BY date) 
                            )
                        WHERE fic = 'USA' AND
                            tpci = '0' AND
                            curcdd = 'USD' AND
                            priusa = (CASE WHEN regexp_full_match(iid, '^[0-9]*$') THEN CAST(iid AS INTEGER) end)
                        )
                    WHERE ttm_mc > 0 AND
                          ttm_min_prccd > 3
                    )
                )
            ORDER BY date
            """

    # making the db connection
    con = SQLConnection(read_only=False).con
    con.execute(sql_ensure_schema_open)
    con.execute(sql_ensure_table_open)
    con.execute(sql_make_rank_universe_table)
    con.close()

    logging.info(f'Finished Ranking Table {table_name}')


def _make_crsp_us_universe_base_table():
    """
    Makes the base table with market cap ranks for each asset. Should be deleted after its done being used
    """
    table_name = 'universe.temp_rank_crsp_mc'
    logging.info(f'Creating Ranking Table {table_name}')

    trading_cal = mcal.get_calendar(
        'NYSE').valid_days(start_date='1925', end_date=pd.to_datetime('today')).to_series().to_frame('trading_days')

    sql_ensure_schema_open = f'CREATE SCHEMA IF NOT EXISTS universe;'
    sql_ensure_table_open = f'DROP TABLE IF EXISTS {table_name};'

    sql_make_rank_universe_table = f""" 
        CREATE TABLE {table_name}
        AS
            SELECT date, permno, permco, ttm_min_prc, ttm_mc, 
                row_number() OVER (PARTITION BY (date) ORDER BY ttm_mc desc) AS ttm_mc_rank
            FROM
                (
                SELECT date, permno, permco, ttm_min_prc, ttm_mc
                FROM
                    (
                    SELECT date, permno, permco, shrcd,
                        AVG(ABS(prc) * shrout) OVER (
                        PARTITION BY permno ORDER BY date ROWS BETWEEN 252 PRECEDING AND CURRENT ROW) AS ttm_mc,
                        MIN(ABS(prc)) OVER (
                        PARTITION BY permno ORDER BY date ROWS BETWEEN 252 PRECEDING AND CURRENT ROW) AS ttm_min_prc
                    FROM 
                        (
                        SELECT date, permno, permco, shrcd,
                        lag(prc, 1, NULL) OVER lagDays AS prc, 
                        lag(shrout, 1, NULL) OVER lagDays AS shrout
                        FROM
                            (
                            SELECT distinct date, permno, permco, shrcd, prc, shrout
                            FROM crsp.sd as sd RIGHT JOIN trading_cal cal on sd.date = cal.trading_days
                            )  
                        WINDOW lagDays AS (
                            PARTITION BY permno
                            ORDER BY date
                        )   
                        )
                    WHERE shrcd = 11
                    )
                WHERE ttm_mc IS NOT NULL AND
                      ttm_min_prc > 3 
                )
            ORDER BY date
        """

    # making the db connection
    con = SQLConnection(read_only=False).con
    con.execute(sql_ensure_schema_open)
    con.execute(sql_ensure_table_open)
    con.execute(sql_make_rank_universe_table)
    con.close()

    logging.info(f'Finished Ranking Table {table_name}')


def clear_master_ranking_table():
    """
    Wipes the ranking tables made by _make_crsp_us_universe_base_table and _make_cstat_us_universe_base_table
    """
    logging.info('Deleting Ranking Tables')

    con = SQLConnection(read_only=False)
    con.execute("DROP SCHEMA universe CASCADE;")
    con.close()

    logging.info('Finished Deleting Ranking Tables')


if __name__ == '__main__':
    # crsp_us_universe(max_rank=500, rebuild_mc_ranking=True, link=True)
    # crsp_us_universe(max_rank=1000, link=True)
    # crsp_us_universe(max_rank=3000, link=True)
    # crsp_us_universe(min_rank=1000, max_rank=3000, link=True)
    #
    # # building compustat universes
    # compustat_us_universe(max_rank=500, rebuild_mc_ranking=True)
    # compustat_us_universe(max_rank=1000)
    # compustat_us_universe(max_rank=3000)
    # compustat_us_universe(min_rank=1000, max_rank=3000)

    # clear_master_ranking_table()
    pass
