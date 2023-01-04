import argparse
from pgConnect import PgConnection
from config import Config
from typing import Dict


def create_player_tables(pgc : PgConnection):
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
    parser = argparse.ArgumentParser()
    parser.add_argument("type", help = "Type can be 'player' or 'box'")
    args = parser.parse_args()
    create_type : str = args.type
    
    conf = Config()
    pgc = PgConnection(conf)

    fx_dict = {'player' : create_player_tables,
                'box' : 'create_box_tables'}
    
    fx = fx_dict[create_type]
    fx(pgc)