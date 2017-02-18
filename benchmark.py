#!/usr/bin/env python


import os
import time
from random import randint
import logging
import pandas
from sqlalchemy import create_engine


class Timer:
    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.end = time.perf_counter()
        self.interval = self.end - self.start


def query_builder(table_name, datatypes_dataframe, rows):
    '''
    Builds and returns a dictionary to be used with the str.format function.
    '''
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
    '''
    Benchmark the database based on the number of iterations specified in the arg['iterations'] parameter.  Execute
    every query from the 'queries_dataframe' parameter against every table in the 'tables_dataframe' parameter.  Write the
    execution time to the 'csv_filepath' parameter.
    '''
    engine = create_engine(attributes['connection_string'])
    for i in range(args['iterations']):
        logging.info("============  Iteration " + str(i+1) +  " ============")
        benchmark_dataframe = pandas.DataFrame()
        for table_index, table_row in tables_dataframe.iterrows():
            logging.info("Querying: " + table_row['table_name'])
            datatypes_query = attributes['datatypes_query'].format(table_name=table_row['table_name'])
            datatypes_dataframe = pandas.read_sql(datatypes_query, engine)
            datatypes_dataframe.columns = map(str.lower, datatypes_dataframe.columns)  # SQLAlchemy column case sensitivity is inconsistent between SQL dialects
            for query_index, query_row in queries_dataframe.iterrows():
                query_builder_dict = query_builder(table_row['table_name'], datatypes_dataframe, args['rows'])
                sql = query_row['query_template'].format(**query_builder_dict)
                with Timer() as t:
                    dataframe = pandas.read_sql(sql, engine)
                    query_row['rows'] = len(dataframe.index)
                logging.info("Query " + str(query_row['query_id']) + str(':  {:f} sec'.format(t.interval)))
                query_row['time'] = float(t.interval)
                query_row['query_executed'] = sql
                query_row['concurrency_factor'] = int(args['concurrent_users'])
                query_row = pandas.concat([query_row, table_row])
                benchmark_dataframe = benchmark_dataframe.append(query_row, ignore_index=True)
        benchmark_dataframe.to_csv(csv_filepath, index=False, mode='a', header=not os.path.isfile(csv_filepath))
        del benchmark_dataframe