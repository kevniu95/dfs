import psycopg2
from configparser import ConfigParser
from typing import Dict


class PgConnection():
    def __init__(self):
        self.conn : psycopg2.connection = self.connect()
        if self.conn:
            self.cursor : psycopg2.cursor = self.conn.cursor
    
    def connect(self):
        """ Connect to the PostgreSQL database server """
        self.conn = None
        try:
            # read connection parameters
            params : Dict[str, str] = self._config()

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            self.conn : psycopg2.connection = psycopg2.connect(**params)
            return self.conn
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        return conn

    def _config(self, filename : str ='database.ini', section : str ='postgresql') -> Dict[str, str]:
        # create a parser
        parser = ConfigParser()
        # read config file
        parser.read(filename)

        # get section, default to postgresql
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, filename))

        return db
    

    def getConn(self):
        return self.conn
    

    def getCurs(self):
        return self.cursor


    def close(self) -> None:
        print("Closing PostgreSQL database...")
        self.conn.close()

if __name__ == '__main__':
    conn = PgConnection()
    conn.close()

