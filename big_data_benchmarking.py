#!/usr/bin/env python3

##########################################################################################
# Written by Justin Naldzin
##########################################################################################


import os
import sys
import json
import logging
from datetime import datetime, timezone
import drop_tables
import create_tables
import query


def main():

    # Configure logging to file and stdout
    start_timestamp = datetime.now(timezone.utc)
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    script_dir = os.path.dirname(os.path.join(os.getcwd(), __file__))
    log_filepath = os.path.join(script_dir, 'log/' + script_name + '.log')
    logging.basicConfig(handlers=[logging.FileHandler(log_filepath, mode='a', encoding='utf-8')],
                        format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
    logging.getLogger().addHandler(logging.StreamHandler())  # logging to stdout
    logging.info("Started " + script_name + " script")

    # Define the benchmarking CSV file
    csv_name = script_name + '_' + start_timestamp.strftime('%Y%m%d') + '.csv'
    csv_filepath = os.path.join(script_dir, 'csv/' + csv_name)
    logging.info("CSV file to save results to: " + csv_filepath)

    # Load database configuration from JSON file
    with open('config.json', 'r') as config_file:
        database_config = json.load(config_file)

    # Create database tables and insert data; Run benchmarks and queries; Drop tables
    '''for database, attributes in database_config.items():
        table_list = create_tables.individual(database, attributes)
        logging.info("Running benchmarks and querying:  " + database)
        getattr(query, attributes['name'])(database, attributes, csv_filepath)
        #logging.info("Dropping tables for:  " + database)
        #drop_tables.drop(database, attributes, table_list)'''

    # Oracle testing
    table_list = create_tables.individual('Oracle Database', database_config['Oracle Database'])
    getattr(query, database_config['Oracle Database']['name'])('Oracle Database', database_config['Oracle Database'], csv_filepath)
    #drop_tables.drop('Oracle Database', database_config['Oracle Database'], table_list)

    # HANA testing
    #table_list = create_tables.individual('HANA', database_config['HANA'])
    #getattr(query, database_config['HANA']['name'])('HANA', database_config['HANA'],csv_filepath)
    #print(table_list)
    #drop_tables.drop('HANA', database_config['HANA'], table_list)

    # SQL Server testing
    #table_list = create_tables.individual('SQL Server', database_config['SQL Server'])
    #getattr(query, database_config['SQL Server']['name'])('SQL Server', database_config['SQL Server'],csv_filepath)
    #print(table_list)
    #drop_tables.drop('SQL Server', database_config['SQL Server'], table_list)

    # Hive testing
    #getattr(query, database_config['Hive']['name'])('Hive', database_config['Hive'],csv_filepath)

    # Plot
    if not os.path.isfile(csv_filepath):
        logging.error("File: " + csv_filepath + " does not exist!")
        logging.info("Exiting!")
        sys.exit(1)
    #bokeh_plot.benchmark(csv_filepath)  # call via a separate function
    import pandas
    from bokeh.charts import Scatter, output_file, show
    dataframe = pandas.read_csv(csv_filepath)
    p = Scatter(dataframe, x='rows', y='time', color="category", title="Time vs Number of rows",
                legend="top_left", legend_sort_field='color', legend_sort_direction='ascending',
                xlabel="Number of rows", ylabel="Seconds")
    output_file("scatter.html", title="Benchmarking Databases")
    show(p)

    # Finish
    logging.info(script_name + " script duration: " + str(datetime.now(timezone.utc) - start_timestamp))
    logging.info("Finished " + script_name + " script")


if __name__ == '__main__':
    main()
