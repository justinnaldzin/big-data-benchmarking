#!/usr/bin/env python


import os
import logging
import subprocess
import pandas
from inflection import underscore
from sqlalchemy import create_engine, types


script_dir = os.path.dirname(os.path.join(os.getcwd(), __file__))
data_path = os.path.join(script_dir + os.path.sep + "data")


def individual(engine, data_filepath_list):
    """
    Create individual SQL tables for every CSV file within a directory
    """
    for filepath in data_filepath_list:
        table_name = os.path.splitext(os.path.basename(filepath))[0]  # set table name to basename of filepath
        logging.info("Reading data file:  " + filepath)
        line_count = sum(1 for line in open('On_Time_Performance_1993_2.csv')) - 1
        logging.info(str(line_count) + " rows will be inserted.")
        for dataframe in pandas.read_csv(filepath, low_memory=False, iterator=True, chunksize=1000000):  # infer dtypes
            dataframe.dropna(axis=1, thresh=len(dataframe.index) // 10, inplace=True)  # drop columns having more than 90% NaN values
            dataframe.columns = map(str.upper, map(underscore, dataframe.columns))  # camelcase to underscore to uppercase
            dtype_dict = {}
            for column in dataframe.columns:  # map DataFrame dtypes to Database data types
                if dataframe[column].dtype in ['object']:
                    dtype_dict[column] = types.String(int(dataframe[column].str.len().max()))
                elif dataframe[column].dtype in ['int64']:
                    dtype_dict[column] = types.Integer()
                elif dataframe[column].dtype in ['float64']:
                    dtype_dict[column] = types.Float()
                else:
                    logging.error(str(dataframe[column].dtype) + " dtype unsupported.  Dropping column:  " + column)  # ['object_', 'string_', 'unicode_', 'unicode', 'int_', 'float_']
                    del dataframe[column]
            logging.info("Creating table and inserting data into table:  " + table_name)
            dataframe.to_sql(table_name, engine, if_exists='append', index=False, chunksize=1000, dtype=dtype_dict)
            logging.info("Successfully inserted data")


############################################################################
# The following function definitions are incomplete
############################################################################


def single(database, attributes):
    """
    Create a single SQL table by combining every CSV file within a directory
    """
    logging.info("Creating a single table for:  " + database)
    dataframes_list = []
    for filename in os.listdir(data_path):
        if filename.endswith(".csv"):
            csv_filepath = os.path.join(data_path, filename)
            logging.info("Reading CSV file:  " + csv_filepath)
            dataframe = pandas.read_csv(csv_filepath, dtype=object)
            dataframes_list.append(dataframe)
    logging.info("Concatenating CSV files into one DataFrame...")
    master_dataframe = pandas.concat(dataframes_list, ignore_index=True)
    logging.info("Establishing connection to " + database + "...")
    engine = create_engine(attributes['connection_string'])
    table_name = os.path.basename(data_path)  #  set table name to data directory name
    logging.info("Inserting data into table:  " + table_name)
    master_dataframe.to_sql(table_name, engine, if_exists='replace', index=True, chunksize=1000)
    logging.info("Successfully inserted data")


def csvkit_individual(database, attributes, execute=False):
    """
    Using csvkit, generate individual SQL CREATE TABLE statements for every CSV file within a directory
    The parameter 'attributes' is a dictionary that includes the string attributes['connection_string'] to use to connect to the database.
    Additionally the string attributes['dialect'] can be used for the dialect of SQL to generate  {firebird,maxdb,informix,mssql,oracle,sybase,sqlite,access,mysql,postgresql}
    The optional boolean parameter 'execute' determines whether to execute the command on the database.
    """
    logging.info("Creating individual tables for:  " + database)
    csv_list = []
    for filename in os.listdir(data_path):
        if filename.endswith(".csv"):
            csv_list.append(filename)
    for csv in csv_list:
        logging.info("Reading CSV file:  " + csv)
        table_name = os.path.splitext(os.path.basename(csv))[0]  # set table name to csv basename
        sql_filename = "CREATE_TABLE_" + table_name + ".sql"
        logging.info("Creating a SQL CREATE TABLE statement for " + csv + " file...")
        if execute:
            bash_cmd = 'cd ' + data_path + '; csvsql ' + attributes['dialect'] + ' --db ' + attributes['connection_string'] + ' --tables ' + table_name + ' --insert ' + csv
        else:
            bash_cmd = 'cd ' + data_path + '; csvsql ' + attributes['dialect'] + ' --tables ' + table_name + ' ' + csv + ' > ' + sql_filename
        logging.info("Calling subprocess:  " + bash_cmd)
        subprocess.call(bash_cmd, shell=True)


def csvkit_single(database, attributes, execute=False):
    """
    Using csvkit, generate a single SQL CREATE TABLE statement by combining every CSV file within a directory
    The parameter 'attributes' is a dictionary that includes the string attributes['connection_string'] to use to connect to the database.
    Additionally the string attributes['dialect'] can be used for the dialect of SQL to generate  {firebird,maxdb,informix,mssql,oracle,sybase,sqlite,access,mysql,postgresql}
    The optional boolean parameter 'execute' determines whether to execute the command on the database.
    """
    logging.info("Creating a single table for:  " + database)
    csv_list = []
    for filename in os.listdir(data_path):
        if filename.endswith(".csv"):
            csv_list.append(filename)
    csv_files = ' '.join(map(str, csv_list))
    logging.info("Reading CSV files:  " + csv_files)
    table_name = os.path.basename(data_path)  # set table name to data directory name
    sql_filename = "CREATE_TABLE_" + table_name + ".sql"
    logging.info("Creating a SQL CREATE TABLE statement...")
    if execute:
        bash_cmd = 'cd ' + data_path + '; csvstack -v --filenames ' + csv_files + ' | csvsql ' + attributes['dialect'] + ' --db ' + attributes['connection_string'] + ' --tables ' + table_name + ' --insert'
    else:
        bash_cmd = 'cd ' + data_path + '; csvstack -v --filenames ' + csv_files + ' | csvsql ' + attributes['dialect'] + ' --tables ' + table_name + ' > ' + sql_filename
    logging.info("Calling subprocess:  " + bash_cmd)
    subprocess.call(bash_cmd, shell=True)
