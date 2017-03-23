#!/usr/bin/env python3

##########################################################################################
# Written by Justin Naldzin
##########################################################################################


import os
import sys
import argparse
import json
import logging
import pandas
from datetime import datetime, timezone
from threading import Thread
from sqlalchemy import create_engine
import drop_tables
import create_tables
import benchmark


script_dir = os.path.dirname(os.path.abspath(__file__))
script_name = os.path.splitext(os.path.basename(__file__))[0]


def initialize_logging(logging_dir):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(module)s %(threadName)s %(message)s')

    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    handler = logging.FileHandler(os.path.join(logging_dir, script_name + '.log'), 'a', encoding='utf-8')
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def main(args):

    # Validate data path
    start_timestamp = datetime.now(timezone.utc)
    logging.info("Started " + script_name + " script")
    data_path = args['data_path']
    if not os.path.isdir(data_path):
        logging.error("No such directory:  " + data_path)
        sys.exit(1)

    # Define the Big Data Benchmarking CSV file
    csv_name = script_name + '.csv'
    csv_filepath = os.path.join(script_dir, 'csv/' + csv_name)

    if not args['database_list']:
        logging.warning('No databases specified.  Skipping benchmarking...')
    else:

        # Load configuration from JSON file
        with open('config.json', 'r') as config_file:
            database_config = json.load(config_file)
        logging.info("Big Data Benchmarking CSV file to save results to:  " + csv_filepath)

        # Iterate through the database list specified in the script arguments
        for database in args['database_list']:
            attributes = database_config.get(database, None)
            if not attributes:
                logging.error("No configuration found for database:  " + database)
                continue
            engine = create_engine(attributes['connection_string'])
            if not args['create_tables']:  # Use existing tables in database
                logging.info('############  Querying ' + database + ' for all table names  ############')
                sql = attributes['table_name_query'].format(table_like=args['table_like'])
                logging.info(sql)
                tables_dataframe = pandas.read_sql(sql, engine)
                tables_dataframe.columns = tables_dataframe.columns.str.lower()  # SQLAlchemy column case sensitivity is inconsistent between SQL dialects
                tables_dataframe.sort_values('table_name', axis=1, inplace=True)
                logging.info('Found the following table names:')
                [logging.info(table_name) for table_name in tables_dataframe['table_name']]
            else:  # Use data files on local file system to create tables and insert into database
                logging.info('############  Searching for data files on local file system  ############')
                data_filepath_list = [os.path.join(data_path, filename) for filename in os.listdir(data_path) if
                                      filename.endswith(".csv")]
                if not data_filepath_list:
                    logging.error("No data files found in path:  " + str(data_path))
                    sys.exit(1)
                tables_dataframe = pandas.DataFrame({'table_name': [(os.path.splitext(os.path.basename(filename))[0])
                                                                    for filename in data_filepath_list]})
                tables_dataframe.sort_values('table_name', axis=1, inplace=True)
                logging.info('Found the following files in path:  ' + str(data_path))
                [logging.info(filename) for filename in data_filepath_list]

                logging.info('############  Create tables and load data into ' + database + '  ############')
                create_tables.individual(engine, data_filepath_list)

                # Alter table
                alter_table_query = attributes.get('alter_table_query', None)
                if alter_table_query:
                    for table_name in tables_dataframe['table_name']:
                        logging.info(alter_table_query.format(table_name=table_name))
                        engine.connect().execute(alter_table_query.format(table_name=table_name))

            # Query for the number of records in each table and categorize
            logging.info('############  Querying for the number of records in each table  ############')
            tables_dataframe['table_row_count'] = tables_dataframe['table_name'].apply(
                lambda table_name: pandas.read_sql('SELECT COUNT(*) FROM "{table_name}"'.format(table_name=table_name),
                                                   engine).ix[:, 0])
            bins = [0, 100000, 1000000, 10000000, 1000000000]
            label_names = ['Small', 'Medium', 'Large', 'X-Large']
            tables_dataframe['table_size_category'] = pandas.cut(tables_dataframe['table_row_count'], bins,
                                                                 labels=label_names)
            logging.info(tables_dataframe[['table_name', 'table_row_count', 'table_size_category']])

            # Benchmark database with concurrent connections
            logging.info('############  Benchmarking ' + database + '  ############')
            queries_filepath = 'queries/queries.csv'
            queries_dataframe = pandas.read_csv(queries_filepath)  # load queries from CSV file
            queries_dataframe = queries_dataframe[
                queries_dataframe['database'] == database]  # filter queries on database name
            if not queries_dataframe.empty:
                with benchmark.Timer() as t:
                    thread_list = [Thread(name=database, target=benchmark.database,
                                          args=(queries_dataframe, attributes, tables_dataframe, csv_filepath, args))
                                   for _ in range(args['concurrent_users'])]
                    [thread.start() for thread in thread_list]
                    [thread.join() for thread in thread_list]
                logging.info(database + ' benchmark time: %.07f sec' % t.interval)
            else:
                logging.warning("Missing " + database + " queries from " + queries_filepath)

            # Drop tables
            if args['drop_tables']:
                logging.info('############  Dropping tables in ' + database + '  ############')
                drop_tables.drop(engine, tables_dataframe)

    # Finish
    logging.info(script_name + " script duration:  " + str(datetime.now(timezone.utc) - start_timestamp))
    logging.info("Finished " + script_name + " script")

    # Bokeh server instructions
    logging.info('------------------------------------------------------------------------------------')
    logging.info('Instruct Bokeh server to launch the web app by typing:')
    logging.info('   bokeh serve app')
    logging.info('Then open your browser and navigate to:  http://localhost:5006')
    logging.info('------------------------------------------------------------------------------------')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Big Data Benchmarking")
    parser.add_argument('database_list', nargs='*', default=[],
                        help="Specify the list of Databases to benchmark.  These names must match the names "
                             "pre-configured in the 'config.json' file.")
    parser.add_argument('-t', '--table-like', dest='table_like', default='%',
                        type=str, help="Specify the name of the tables to benchmark.  This uses the SQL 'LIKE' operator"
                                       " to search a specified pattern so use the '%%' sign to define wildcards.  "
                                       "Default is '%%' which will find all existing tables in the database.")
    parser.add_argument('-r', '--rows', dest='rows', default=10000, type=int,
                        help="The maximum number of rows to return from each query execution.  Default is 10000")
    parser.add_argument('-i', '--iterations', dest='iterations', default=1, type=int,
                        help="The number of benchmark iterations to perform on the database.  Default is 1")
    parser.add_argument('-u', '--users', dest='concurrent_users', default=1, type=int,
                        help="The number of concurrent users to connect to the database.  Default is 1")
    parser.add_argument('-p', '--path', dest='data_path', default=os.path.join(script_dir + os.path.sep + 'data'),
                        type=str, help="Full directory path to where the data files are stored.  These will be used to "
                                       "create the tables and insert into database.  Default path is:  /data")
    parser.add_argument('-c', '--create-tables', dest='create_tables', action='store_true',
                        help="Create tables and insert into database the data files that exist within in the folder "
                             "'--path' argument.  Not specifying this option will run benchmarks on all existing "
                             "tables in the database.")
    parser.add_argument('-d', '--drop-tables', dest='drop_tables', action='store_true',
                        help="The '--create-tables' argument must be specified.  Only those tables created will be dropped.")

    args = vars(parser.parse_args())
    if args['drop_tables'] and not args['create_tables']:
        parser.error("[-d], [--drop-tables] requires [-c], [--create-tables].  Use [-h] for more help.")
    initialize_logging(os.path.join(script_dir, 'log/'))
    main(args)
