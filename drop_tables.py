import logging


def drop(engine, tables_dataframe):
    '''
    Drop all SQL tables specified in the list parameter 'table_list'
    '''
    connection = engine.connect()
    for table_name in tables_dataframe['table_name']:
        sql = 'DROP TABLE "' + table_name + '"'
        logging.info("Dropping table: " + table_name)
        connection.execute(sql)
    connection.close()
