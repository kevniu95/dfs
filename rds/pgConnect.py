from typing import Dict
import psycopg2
from psycopg2.extensions import connection, cursor

from config import Config


class PgConnection():
    def __init__(self, config):
        self.config = config
        self.conn : connection = self._connect()
        if self.conn:
            self.cursor : cursor = self.conn.cursor()
            
    def _connect(self) -> connection:
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
    

    def getCurs(self) -> cursor: # cursor is builtin_function
        return self.cursor


    def close(self) -> None:
        if self.conn:
            print("Closing PostgreSQL database...")
            self.conn.close()

if __name__ == '__main__':
    config = Config('config.ini')
    pgc = PgConnection(config)
    conn = pgc.getConn()
    cur = pgc.getCurs()

    # conn = PgConnection(config)
    conn.close()