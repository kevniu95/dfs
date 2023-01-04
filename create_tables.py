from psycopg2.extensions import connection
from pgConnect import PgConnection
from config import Config
from typing import Dict


def create_tables(pgc : PgConnection):
    conn = pgc.getConn()
    cur  = pgc.getCurs()
    commands = (
                """
                CREATE TABLE teams (
                    season INTEGER,
                    team VARCHAR(255),
                    stadium VARCHAR(255),
                    PRIMARY KEY (season, team)
                )
                """,
                """
                CREATE TABLE players(
                    player_id SERIAL PRIMARY KEY,
                    player_name VARCHAR(255),
                    height INTEGER,
                    weight INTEGER,
                    birthday DATE,
                    country VARCHAR(10),
                    start_year INTEGER,
                    college VARCHAR(10)
                )
                """,
                """
                CREATE TABLE rosters(
                    season INTEGER,
                    team VARCHAR(255),
                    number INTEGER,
                    player_id integer REFERENCES players,
                    position VARCHAR(10)
                )
                """
    )
    for command in commands:
        print("Executing command...")
        cur.execute(command)
    
    cur.close()
    conn.commit()
    conn.close()
    return



if __name__ == '__main__':
    conf = Config()
    pgc = PgConnection(conf)
    create_tables(pgc)
    