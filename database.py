import sys
import logging


'''Usage:
from database import Database
db = Database(attributes)
db.connect()
sql = "SELECT * FROM TABLE"
dataframe = pandas.read_sql(sql, db.connection)
'''


class Database:
    def __init__(self, config):
        self.name = config['name']
        self.host = config['host']
        self.user = config['user']
        self.password = config['password']
        self.port = config['port']
        self.service_name = config.get('service_name', '')
        self.interface = config['interface']

        self.db = __import__(self.interface)
        if self.service_name:
            self.dsn = self.db.makedsn(self.host, self.port, service_name=self.service_name)  # Create data source name string (Oracle only)

    def connect(self):
        try:
            if not self.service_name:
                self.connection = self.db.connect(host=self.host, port=self.port, user=self.user, password=self.password)
            else:
                self.connection = self.db.connect(user=self.user, password=self.password, dsn=self.dsn)
                self.cursor = self.connection.cursor()
            logging.info("Database connection established")
        except self.db.Error as e:
            logging.error("Database connection failed: {}".format(e))
            logging.info("Exiting!")
            sys.exit(1)


    def query(self, sql):
        self.cursor.execute(sql)
        self.sqlout = self.cursor.fetchall()


    def __del__(self):
        #self.cursor.close()
        self.connection.close()
        logging.info("Database connection closed")
