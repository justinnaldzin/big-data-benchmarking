#!/usr/bin/env python


import os
import sys
import time
from random import randint
import logging
from parse_config import read_config
import pandas, numpy
from sqlalchemy import create_engine


script_dir = os.path.dirname(os.path.join(os.getcwd(), __file__))
queries_dir = "queries/oracle"
queries_path = os.path.join(script_dir + os.path.sep + queries_dir)


class Timer:
    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.end = time.perf_counter()
        self.interval = self.end - self.start


def query_builder(table, datatypes_dataframe):
    '''
    Builds and returns a dictionary to be used with the str.format function.
    '''
    clob_columns = datatypes_dataframe[datatypes_dataframe['DATA_TYPE'] == 'CLOB']['COLUMN_NAME']
    character_columns = datatypes_dataframe[datatypes_dataframe['DATA_TYPE'] != 'CLOB']['COLUMN_NAME']
    number_columns = datatypes_dataframe[datatypes_dataframe['DATA_TYPE'].isin(['NUMBER', 'FLOAT'])]['COLUMN_NAME']  # Oracle
    number_columns = datatypes_dataframe[datatypes_dataframe['DATA_TYPE'].isin(['INTEGER', 'BIGINT'])]['COLUMN_NAME']  # HANA
    number_columns = datatypes_dataframe[datatypes_dataframe['DATA_TYPE'].isin(['tinyint', 'smallint', 'int', 'bigint', 'decimal', 'numeric', 'smallmoney', 'money', 'float', 'real'])]['COLUMN_NAME']  # SQL Server

    Oracle_numeric_datatypes = ['NUMBER', 'FLOAT']
    HANA_numeric_datatypes = ['TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'SMALLDECIMAL', 'DECIMAL', 'REAL', 'DOUBLE']
    SQL_Server_numeric_datatypes = ['tinyint', 'smallint', 'int', 'bigint', 'decimal', 'numeric', 'smallmoney', 'money', 'float', 'real']

    query_builder_dict = {}
    query_builder_dict['columns'] = '"' + '", "'.join(map(str, list(character_columns.sample(n=randint(1, character_columns.size))))) + '"'
    query_builder_dict['table'] = '"' + table + '"'
    query_builder_dict['column_1'] = '"' + character_columns.sample().to_string(header=False, index=False) + '"'
    query_builder_dict['column_2'] = '"' + character_columns.sample().to_string(header=False, index=False) + '"'
    query_builder_dict['row'] = str(randint(50, 5000))
    query_builder_dict['agg_column'] = '"' + number_columns.sample().to_string(header=False, index=False) + '"'
    query_builder_dict['order_column'] = '"' + character_columns.sample().to_string(header=False, index=False) + '"'
    query_builder_dict['analytic_column'] = '"' + number_columns.sample().to_string(header=False, index=False) + '"'
    query_builder_dict['table1'] = '"' + table + '"'
    query_builder_dict['table2'] = '"' + table + '"'
    query_builder_dict['column'] = '"' + character_columns.sample().to_string(header=False, index=False) + '"'
    return query_builder_dict


def benchmark(conn, tables_dataframe, queries_dataframe):
    '''
    For every table supplied in the tables_dataframe parameter, execute the queries in the queries_dataframe parameter
    and return a dataframe containing the execution time it takes.
    '''
    print(tables_dataframe.columns)
    tables_dataframe.columns = map(str.upper, tables_dataframe.columns)  # bug:  HANA returns lowercase columns
    tables_dataframe = tables_dataframe[tables_dataframe.TABLE_NAME.str.startswith('On_Time_Performance_')]  # TESTING: Filter for specific table names
    benchmark_dataframe = pandas.DataFrame()
    for table_index, table_row in tables_dataframe.iterrows():
        table = str(table_row['TABLE_NAME'])
        print(table)

        # Query table for datatypes (LONG, NUMBER, TIMESTAMP, VARCHAR2, BLOB, CHAR, CLOB, DATE, FLOAT) and use columns for query builder
        datatypes_query = "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = '" + table + "'"  # Oracle
        datatypes_query = "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE_NAME AS DATA_TYPE, LENGTH AS DATA_LENGTH FROM TABLE_COLUMNS WHERE SCHEMA_NAME = 'NALDZINJ' AND TABLE_NAME = '" + table + "'"  # HANA
        datatypes_query = "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, DATETIME_PRECISION, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + table + "'"  # SQL Server
        datatypes_dataframe = pandas.read_sql(datatypes_query, conn)
        datatypes_dataframe.columns = map(str.upper, datatypes_dataframe.columns)  # bug:  HANA returns lowercase columns

        # Execute every query and store the execution time
        for query_index, query_row in queries_dataframe.iterrows():
            query_builder_dict = query_builder(table, datatypes_dataframe)
            sql = query_row['query_template'].format(**query_builder_dict)
            with Timer() as t:
                dataframe = pandas.read_sql(sql, conn)
                query_row['rows'] = len(dataframe.index)
            print('%.07f sec' % t.interval)
            query_row['table'] = table
            query_row['time'] = t.interval
            query_row['query_executed'] = sql
            benchmark_dataframe = benchmark_dataframe.append(query_row, ignore_index=True)
    return benchmark_dataframe


def oracle_database(database, attributes, csv_filepath):

    # Define Database object
    from database import Database
    db = Database(attributes)

    # Connect to Database
    db.connect()

    # Retrieve all table names
    sql = "SELECT TABLE_NAME, TABLESPACE_NAME FROM USER_TABLES"
    tables_dataframe = pandas.read_sql(sql, db.connection)

    # Read in sample queries
    queries_csv_filepath = 'queries/queries.csv'
    queries_dataframe = pandas.read_csv(queries_csv_filepath)
    queries_dataframe = queries_dataframe[queries_dataframe['database'] == database]  # filter queries on Database name

    # Benchmark the database
    iterations = 2
    with Timer() as t:
        for _ in range(iterations):
            benchmark_dataframe = benchmark(db.connection, tables_dataframe, queries_dataframe)
            benchmark_dataframe.to_csv(csv_filepath, index=False, mode='a', header=not os.path.isfile(csv_filepath))
    print('Benchmark time: %.07f sec.' % t.interval)

def oracle_database_in_memory(configs):
    pass


def sql_server(database, attributes, csv_filepath):

    # Connect to Database
    logging.info("Establishing connection to " + database + "...")
    engine = create_engine(attributes['connection_string'])

    # Retrieve all table names
    logging.info("Retrieving table names from " + database + "...")
    sql = "SELECT TABLE_CATALOG, TABLE_SCHEMA AS SCHEMA_NAME, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG = 'dac_ariline_testing'"
    tables_dataframe = pandas.read_sql(sql, engine)

    # Read in sample queries
    queries_csv_filepath = 'queries/queries.csv'
    queries_dataframe = pandas.read_csv(queries_csv_filepath)
    queries_dataframe = queries_dataframe[queries_dataframe['database'] == database]  # filter queries on Database name

    # Benchmark the database
    if not queries_dataframe.empty and not tables_dataframe.empty:
        iterations = 2
        with Timer() as t:
            for _ in range(iterations):
                benchmark_dataframe = benchmark(engine, tables_dataframe, queries_dataframe)
                benchmark_dataframe.to_csv(csv_filepath, index=False, mode='a', header=not os.path.isfile(csv_filepath))
        print('Benchmark time: %.07f sec.' % t.interval)
    else:
        logging.warning("Missing " + database + " queries from the CSV")


def hive(configs):
    import pyhive


def hana(database, attributes, csv_filepath):

    # Connect to Database
    logging.info("Establishing connection to " + database + "...")
    engine = create_engine(attributes['connection_string'])

    # Retrieve all table names
    logging.info("Retrieving table names from " + database + "...")
    sql = "SELECT TABLE_NAME, SCHEMA_NAME FROM TABLES WHERE SCHEMA_NAME = 'NALDZINJ'"
    tables_dataframe = pandas.read_sql(sql, engine)

    # Read in sample queries
    queries_csv_filepath = 'queries/queries.csv'
    queries_dataframe = pandas.read_csv(queries_csv_filepath)
    queries_dataframe = queries_dataframe[queries_dataframe['database'] == database]  # filter queries on Database name

    # Benchmark the database
    if not queries_dataframe.empty and not tables_dataframe.empty:
        iterations = 2
        with Timer() as t:
            for _ in range(iterations):
                benchmark_dataframe = benchmark(engine, tables_dataframe, queries_dataframe)
                benchmark_dataframe.to_csv(csv_filepath, index=False, mode='a', header=not os.path.isfile(csv_filepath))
        print('Benchmark time: %.07f sec.' % t.interval)
    else:
        logging.warning("Missing " + database + " queries from the CSV")

def spark_sql(configs):
    pass
