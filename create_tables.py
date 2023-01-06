import argparse
from pgConnect import PgConnection
from config import Config
from typing import Dict


def create_player_tables(pgc : PgConnection):
    conn = pgc.getConn()
    cur  = pgc.getCurs()
    commands = (
                """
                CREATE TABLE team (
                    season INTEGER NOT NULL,
                    team VARCHAR(255) NOT NULL,
                    stadium VARCHAR(255),
                    PRIMARY KEY (season, team),
                    UNIQUE (season, team)
                )
                """,
                """
                CREATE TABLE player(
                    player_name VARCHAR(255) NOT NULL,
                    dob DATE NOT NULL,
                    height INTEGER NOT NULL,
                    weight INTEGER NOT NULL,
                    draft INTEGER,
                    debut_season INTEGER,
                    country VARCHAR(10),
                    college VARCHAR(255),
                    link VARCHAR(255),
                    PRIMARY KEY(player_name, dob, height, weight),
                    UNIQUE(player_name, dob, height, weight)
                )
                """,
                """
                CREATE TABLE roster(
                    season INTEGER,
                    team VARCHAR(255),
                    player_name VARCHAR(255) NOT NULL,
                    dob DATE NOT NULL,
                    height INTEGER NOT NULL,
                    weight INTEGER NOT NULL,
                    jersey_number INTEGER,
                    position VARCHAR(10),
                    
                    PRIMARY KEY(season, team, player_name, dob, height, weight),
                    UNIQUE(season, team, player_name, dob, height, weight),
                    FOREIGN KEY(player_name, dob, height, weight) 
                        REFERENCES player(player_name, dob, height, weight),
                    FOREIGN KEY(season, team)
                        REFERENCES team(season, team)
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