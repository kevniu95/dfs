import psycopg2
from psycopg2.extensions import connection
from configparser import ConfigParser
from typing import Dict, Callable
from config import Config


class PgConnection():
    def __init__(self, config):
        self.config = config
        self.conn : connection = self.connect()
        if self.conn:
            self.cursor : psycopg2.cursor = self.conn.cursor
            
    def connect(self) -> connection:
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # read connection parameters
            params : Dict[str, str] = self.config.parse_section('postgresql')

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn : psycopg2.connection = psycopg2.connect(**params)
            return conn
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        return conn

    def getConn(self) -> connection:
        return self.conn
    

    def getCurs(self) -> Callable: # cursor is builtin_function
        return self.cursor


    def close(self) -> None:
        if self.conn:
            print("Closing PostgreSQL database...")
            self.conn.close()

if __name__ == '__main__':
    config = Config('config.ini')
    conn = PgConnection(config)
    conn.close()