import logging
from sqlalchemy import create_engine


def drop(database, attributes, table_list):
    '''
    Drop SQL tables given the parameter 'table_list'
    '''
    logging.info("Establishing connection to " + database + "...")
    engine = create_engine(attributes['connection_string'])
    connection = engine.connect()
    for table_name in table_list:
        sql = 'DROP TABLE "' + table_name + '"'
        logging.info("Dropping table: " + table_name)
        logging.info(sql)
        connection.execute(sql)
    connection.close()