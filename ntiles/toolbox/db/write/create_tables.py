import logging
from typing import Dict, List

from ntiles.toolbox.db.api.sql_connection import SQLConnection

logging.basicConfig(format='%(message)s ::: %(asctime)s', datefmt='%I:%M:%S %p', level=logging.INFO)


class IngestDataBase:
    def __init__(self, connection_string: str = None):
        """
        :param connection_string: optional string connection to the database
            if none is given then will fall back onto path in settings.py
        """
        self._sql_api = SQLConnection(connection_string=connection_string, read_only=False)

    def ingest(self, to_insert: List[Dict[str, str]], overwrite: bool = False, rows_to_interpret: int = 5_000,
               close: bool = True) -> None:
        """
        will ingest the files specified by to_insert
        :param to_insert: A dictionary containing the schema, tablename and file path for a
            table that should be inserted into the db
            [{
            'schema': 'sch1',
            'table': 'tbl1',
            'file_path': 'full/path/to/file',
            'custom': "UPDATE sch1.tbl1 SET LINKENDDT=99991231 WHERE LINKENDDT = 'E';",
            'rename': {'datadate': 'date'},
            'alter_type': {'gsector': 'VARCHAR', 'date': ['timestamp', '%Y%m%d']},
            'index': [{'name': 'ixd2', 'column': 'col1'},  {'name': 'idx2', 'column': 'col2'}]
            'where': "date > '2000'"
            'from': "AS data JOIN crsp.crsp_cstat_link as link on data.permno = link.lpermno"
            'rows_to_interpret': 500_000
            }]
        :param overwrite: should the tables be overwritten if they exist?
        :param rows_to_interpret: how many rows should we read to determine the types
        :param close: should we close the sql connection after everything is inserted?
        :return: None
        """
        try:
            for tbl_to_create in to_insert:
                logging.info(f'Inserting {tbl_to_create["schema"]}.{tbl_to_create["table"]}')
                self._create_schema(tbl_to_create)  # creates schema
                self._drop(tbl_to_create, overwrite)  # drops tbl if user wants to
                self._create_tbl(tbl_to_create, rows_to_interpret)  # writing table
                self._custom_sql(tbl_to_create)  # letting the user run any sql code
                self._rename_columns(tbl_to_create)  # renaming columns
                self._alter_types(tbl_to_create)  # changing types of data
                self._to_lowercase(tbl_to_create)  # making all column names lowercase
                self._create_index(tbl_to_create)  # making indexes

        except Exception as e:
            self._sql_api.close()
            raise e

        if close:
            self._sql_api.close()
            logging.info('Closed SQL Connection')

    def _create_schema(self, tbl_to_create) -> None:
        """
        :param tbl_to_create: dict defining the table we want to create
        :return: None
        """
        sql_query = f"""CREATE SCHEMA IF NOT EXISTS {tbl_to_create['schema']};"""
        self._sql_api.execute(sql_query)

    def _drop(self, tbl_to_create, overwrite) -> None:
        """
        Drpos a table if it exists and the user wants to drop the table
        :param tbl_to_create: dict defining the table we want to drop
        :param overwrite: should we drop the table?
        :return: None
        """
        if overwrite:
            tbl_name = self._get_table_name(tbl_to_create)
            sql_query = f"""DROP TABLE IF EXISTS {tbl_name};"""
            self._sql_api.execute(sql_query)

    def _create_tbl(self, tbl_to_create, rows_to_interpret) -> None:
        """
        inserts a table into the specified schema and table name
        no adjustments are done to the table or types declared
        :param tbl_to_create: dict defining the table we want to create
        :return: None
        """
        tbl_name = self._get_table_name(tbl_to_create)

        rows_to_interpret = tbl_to_create[
            'rows_to_interpret'] if 'rows_to_interpret' in tbl_to_create else rows_to_interpret

        where_clause = f"WHERE {tbl_to_create.get('where')}" if tbl_to_create.get('where') else ''
        from_clause = tbl_to_create.get('from') if tbl_to_create.get('from') else ''

        sql_query = f"""
            CREATE TABLE {tbl_name} AS 
                SELECT * 
                FROM  read_csv_auto('{tbl_to_create['file_path']}', SAMPLE_SIZE={rows_to_interpret}) {from_clause}
                {where_clause}"""

        self._sql_api.execute(sql_query)

        logging.info(f'\tCreated table {tbl_to_create["schema"]}.{tbl_to_create["table"]}')

    def _custom_sql(self, tbl_to_create):
        """
        lets the user run any sql code they want
        :param tbl_to_create: dict defining the table we want to create
        :return: None
        """

        if 'custom' not in tbl_to_create:
            return

        self._sql_api.execute(tbl_to_create['custom'])

        logging.info('\tRan custom sql code')

    def _rename_columns(self, tbl_to_create) -> None:
        """
        renames the columns specified by the user
        :param tbl_to_create: dict defining the table we want to create
        :return: None
        """
        # if there are no columns to rename then return
        if 'rename' not in tbl_to_create:
            return

        tbl_name = self._get_table_name(tbl_to_create)

        for col_to_rename in tbl_to_create['rename']:
            sql_query = f"""ALTER TABLE {tbl_name} RENAME COLUMN 
                        {col_to_rename} TO {tbl_to_create['rename'][col_to_rename]};"""
            self._sql_api.execute(sql_query)
            logging.info(f'\tRenamed {col_to_rename} -> {tbl_to_create["rename"][col_to_rename]}')

    def _alter_types(self, tbl_to_create) -> None:
        """
        alters the types of columns according to the user
        :param tbl_to_create: dict defining the table we want to create
        :return: None
        """
        # if there are no columns to alter types then return
        if 'alter_type' not in tbl_to_create:
            return

        tbl_name = self._get_table_name(tbl_to_create)

        for col_to_alter in tbl_to_create['alter_type']:
            # should we do a timestamp parse?
            if tbl_to_create['alter_type'][col_to_alter][0] == 'timestamp':
                date_format = tbl_to_create['alter_type'][col_to_alter][1]
                sql_query = f"""ALTER TABLE {tbl_name} ALTER {col_to_alter} TYPE varchar; 
                                ALTER TABLE {tbl_name} ALTER {col_to_alter} SET DATA TYPE 
                                TIMESTAMP USING strptime({col_to_alter}, '{date_format}')"""

            else:
                sql_query = f"""ALTER TABLE {tbl_name} ALTER {col_to_alter} TYPE 
                                {tbl_to_create['alter_type'][col_to_alter]};"""

            self._sql_api.execute(sql_query)
            logging.info(f'\tAltered column {col_to_alter}')

    def _create_index(self, tbl_to_create) -> None:
        """
        creates indexes for a table
        :param tbl_to_create: dict defining the table we want to create
        :return: None
        """

        # if there are no columns to index then return
        if 'index' not in tbl_to_create:
            return

        tbl_name = self._get_table_name(tbl_to_create)

        for idx in tbl_to_create['index']:
            sql_query = f"""CREATE INDEX {idx['name']} ON {tbl_name} ({idx['column']});"""
            self._sql_api.execute(sql_query)

            logging.info(f'\tCreated index {idx["name"]} using {idx["column"]}')

    def _to_lowercase(self, tbl_to_create) -> None:
        """
        turns all columns in a table to lowercase
        :param tbl_to_create: dict defining the table we want to create
        :return: None
        """
        tbl_name = self._get_table_name(tbl_to_create)

        cols = self._sql_api.execute(f'PRAGMA table_info({tbl_name})').fetchdf().name

        for col in cols:
            self._sql_api.execute(f"""ALTER TABLE {tbl_name} RENAME COLUMN "{col}" TO "{col.lower()}";""")

        logging.info('\tSuccessfully made all columns lowercase')

    @staticmethod
    def _get_table_name(tbl_to_create) -> str:
        """
        gets the table name for a tbl_to_create
        :param tbl_to_create: dict defining the table we want to create
        :return: table name
        """
        return f"{tbl_to_create['schema']}.{tbl_to_create['table']}"
