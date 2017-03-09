#!/usr/bin/env python


import os
import sys
import time
import logging
import threading
import pandas
from random import randint
from retrying import retry
from sqlalchemy import create_engine


class Timer:
    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.end = time.perf_counter()
        self.interval = self.end - self.start


class Timeout(Exception):
    pass


def timelimit(timeout):
    def internal(function):
        def internal2(*args, **kw):
            class Calculator(threading.Thread):
                def __init__(self):
                    threading.Thread.__init__(self)
                    self.result = None
                    self.error = None

                def run(self):
                    try:
                        self.result = function(*args, **kw)
                    except:
                        self.error = sys.exc_info()[0]
            c = Calculator()
            c.start()
            c.join(timeout)
            if c.isAlive():
                raise Timeout
            if c.error:
                raise c.error
            return c.result
        return internal2
    return internal


@retry(stop_max_attempt_number=6)  # stop after 6 attempts
@timelimit(3600)  # timeout after 600 seconds (10 minutes)
def query(engine, query_template, table_name, datatypes_dataframe, rows):
    query_builder_dict = query_builder(table_name, datatypes_dataframe, rows)
    sql = query_template.format(**query_builder_dict)
    logging.debug(sql)
    with Timer() as t:
        dataframe = pandas.read_sql(sql, engine)
        rows = len(dataframe.index)
    return sql, rows, float(t.interval)


def query_builder(table_name, datatypes_dataframe, rows):
    """
    Builds and returns a dictionary to be used with the str.format function.
    """
    oracle_numeric_datatypes = ['NUMBER', 'FLOAT', 'INTEGER']
    hana_numeric_datatypes = ['TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'SMALLDECIMAL', 'DECIMAL', 'REAL', 'DOUBLE']
    sqlserver_numeric_datatypes = ['tinyint', 'smallint', 'int', 'bigint', 'decimal', 'numeric', 'float']
    numeric_datatypes = set(map(str.lower, (oracle_numeric_datatypes + hana_numeric_datatypes + sqlserver_numeric_datatypes)))
    numeric_columns = datatypes_dataframe[datatypes_dataframe['data_type'].str.lower().isin(numeric_datatypes)]['column_name']
    character_columns = datatypes_dataframe[~datatypes_dataframe['data_type'].str.lower().isin(numeric_datatypes)]['column_name']  # non-numeric columns
    query_builder_dict = {}
    query_builder_dict['columns'] = '"' + '", "'.join(map(str, list(character_columns.sample(n=randint(1, character_columns.size))))) + '"'
    query_builder_dict['table'] = '"' + table_name + '"'
    query_builder_dict['column_1'] = '"' + character_columns.sample().to_string(header=False, index=False) + '"'
    query_builder_dict['column_2'] = '"' + character_columns.sample().to_string(header=False, index=False) + '"'
    query_builder_dict['row'] = str(randint(1, rows))
    query_builder_dict['order_column'] = '"' + character_columns.sample().to_string(header=False, index=False) + '"'
    query_builder_dict['numeric_column'] = '"' + numeric_columns.sample().to_string(header=False, index=False) + '"'
    query_builder_dict['column'] = '"' + character_columns.sample().to_string(header=False, index=False) + '"'
    return query_builder_dict


def database(queries_dataframe, attributes, tables_dataframe, csv_filepath, args):
    """
    Benchmark the database based on the number of iterations specified in the arg['iterations'] parameter.  Execute
    every query from the 'queries_dataframe' parameter against every table in the 'tables_dataframe' parameter.  Write the
    execution time to the 'csv_filepath' parameter.
    """
    engine = create_engine(attributes['connection_string'])
    for i in range(args['iterations']):
        logging.info("============  Iteration " + str(i+1) + " ============")
        benchmark_dataframe = pandas.DataFrame()
        for table_index, table_row in tables_dataframe.iterrows():
            logging.info("Querying: " + table_row['table_name'])
            datatypes_query = attributes['datatypes_query'].format(table_name=table_row['table_name'])
            datatypes_dataframe = pandas.read_sql(datatypes_query, engine)
            datatypes_dataframe.columns = map(str.lower, datatypes_dataframe.columns)  # SQLAlchemy column case sensitivity is inconsistent between SQL dialects
            for query_index, query_row in queries_dataframe.iterrows():
                try:
                    (query_row['query_executed'], query_row['rows'], query_row['time']) = query(engine, query_row['query_template'], table_row['table_name'], datatypes_dataframe, args['rows'])
                    logging.info("Query " + str(query_row['query_id']) + str(':  {:f} sec'.format(query_row['time'])))
                except Timeout:
                    (query_row['query_executed'], query_row['rows'], query_row['time']) = ('Timeout!', 0, 600)
                    logging.warning("Timeout!  " + "Query " + str(query_row['query_id']) + str(':  {:f} sec'.format(query_row['time'])))
                query_row['concurrency_factor'] = int(args['concurrent_users'])
                query_row = pandas.concat([query_row, table_row])
                benchmark_dataframe = benchmark_dataframe.append(query_row, ignore_index=True)
        benchmark_dataframe.to_csv(csv_filepath, index=False, mode='a', header=not os.path.isfile(csv_filepath))
        del benchmark_dataframe