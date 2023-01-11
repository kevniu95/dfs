import argparse
from pgConnect import PgConnection
from config import Config
from typing import Dict, List, Tuple


# Season
# season  /  team_name  /  stadium_name

# Rosters 
# season / team_name / player_id / player_name / Pos / Exp

# Players
# player_id / player_name / birth_date / country / start_yr / college

# Draft
# year / pick_num / team / player / college / link

# team_box

# player_box


def create_boxscore_tables(pgc : PgConnection):
    commands = ("""
                CREATE TABLE team_box(
                    game_date DATE NOT NULL,
                    game_time TIME NOT NULL,
                    attendance INTEGER NOT NULL,
                    arena VARCHAR(255) NOT NULL,
                    tm1 VARCHAR(255) NOT NULL,
                    home INTEGER NOT NULL,
                    FGM INTEGER NOT NULL,
                    FGA INTEGER NOT NULL,
                    FG_pct FLOAT,
                    TPM INTEGER NOT NULL,
                    TPA INTEGER NOT NULL,
                    TP_pct FLOAT,
                    FTM INTEGER NOT NULL,
                    FTA INTEGER NOT NULL,
                    FT_pct FLOAT,
                    ORB INTEGER NOT NULL,
                    DRB INTEGER NOT NULL,
                    TRB INTEGER NOT NULL,
                    AST INTEGER NOT NULL,
                    STL INTEGER NOT NULL,
                    BLK INTEGER NOT NULL,
                    TOV INTEGER NOT NULL,
                    PF INTEGER NOT NULL,
                    PTS INTEGER NOT NULL,
                    TS_pct FLOAT ,
                    eFG_pct FLOAT NOT NULL,
                    TPAr FLOAT NOT NULL,
                    FTr FLOAT NOT NULL,
                    ORB_pct FLOAT,
                    DRB_pct FLOAT,
                    TRB_pct FLOAT,
                    AST_pct FLOAT,
                    STL_pct FLOAT,
                    BLK_pct FLOAT,
                    TOV_pct FLOAT,
                    USG_pct FLOAT,
                    ORtg FLOAT NOT NULL,
                    DRtg FLOAT NOT NULL,
                    tm2 VARCHAR(255) NOT NULL,
                    home_a INTEGER NOT NULL,
                    FGM_a INTEGER NOT NULL,
                    FGA_a INTEGER NOT NULL,
                    FG_pct_a FLOAT,
                    TPM_a INTEGER NOT NULL,
                    TPA_a INTEGER NOT NULL,
                    TP_pct_a FLOAT,
                    FTM_a INTEGER NOT NULL,
                    FTA_a INTEGER NOT NULL,
                    FT_pct_a FLOAT,
                    ORB_a INTEGER NOT NULL,
                    DRB_a INTEGER NOT NULL,
                    TRB_a INTEGER NOT NULL,
                    AST_a INTEGER NOT NULL,
                    STL_a INTEGER NOT NULL,
                    BLK_a INTEGER NOT NULL,
                    TOV_a INTEGER NOT NULL,
                    PF_a INTEGER NOT NULL,
                    PTS_a INTEGER NOT NULL,
                    TS_pct_a FLOAT ,
                    eFG_pct_a FLOAT ,
                    TPAr_a FLOAT NOT NULL,
                    FTr_a FLOAT NOT NULL,
                    ORB_pct_a FLOAT,
                    DRB_pct_a FLOAT,
                    TRB_pct_a FLOAT,
                    AST_pct_a FLOAT,
                    STL_pct_a FLOAT,
                    BLK_pct_a FLOAT,
                    TOV_pct_a FLOAT,
                    USG_pct_a FLOAT,
                    ORtg_a FLOAT NOT NULL,
                    DRtg_a FLOAT NOT NULL,
                    PRIMARY KEY (game_date, game_time, tm1, tm2),
                    UNIQUE (game_date, game_time, tm1, tm2)
                )
                """,
                """
                CREATE TABLE player_box(
                    game_date DATE NOT NULL,
                    game_time TIME NOT NULL,
                    tm1 VARCHAR(255) NOT NULL,
                    tm2 VARCHAR(255) NOT NULL,
                    player_name VARCHAR(255) NOT NULL,
                    
                    mins_played FLOAT NOT NULL,
                    FGM INTEGER NOT NULL,
                    FGA INTEGER NOT NULL,
                    FG_pct FLOAT,
                    TPM INTEGER NOT NULL,
                    TPA INTEGER NOT NULL,
                    TP_pct FLOAT,
                    FTM INTEGER NOT NULL,
                    FTA INTEGER NOT NULL,
                    FT_pct FLOAT,
                    ORB INTEGER NOT NULL,
                    DRB INTEGER NOT NULL,
                    TRB INTEGER NOT NULL,
                    AST INTEGER NOT NULL,
                    STL INTEGER NOT NULL,
                    BLK INTEGER NOT NULL,
                    TOV INTEGER NOT NULL,
                    PF INTEGER NOT NULL,
                    PTS INTEGER NOT NULL,
                    PlusMinus FLOAT NOT NULL,

                    TS_pct FLOAT,
                    eFG_pct FLOAT,
                    TPAr FLOAT NOT NULL,
                    FTr FLOAT NOT NULL,
                    ORB_pct FLOAT,
                    DRB_pct FLOAT,
                    TRB_pct FLOAT,
                    AST_pct FLOAT,
                    STL_pct FLOAT,
                    BLK_pct FLOAT,
                    TOV_pct FLOAT,
                    USG_pct FLOAT,
                    ORtg FLOAT NOT NULL,
                    DRtg FLOAT NOT NULL,
                    BPM FLOAT NOT NULL,
                    PRIMARY KEY(game_date, game_time, tm1, tm2, player_name),
                    UNIQUE (game_date, game_time, tm1, tm2, player_name)
                )
                """,
                )
    _exec_command(pgc,commands)

def create_draft_table(pgc : PgConnection):
    commands = (
                """
                CREATE TABLE draft(
                    year INTEGER NOT NULL,
                    pick INTEGER NOT NULL,
                    team VARCHAR(10) NOT NULL,
                    player VARCHAR(255) NOT NULL,
                    college VARCHAR(255),
                    link VARCHAR(255),
                    PRIMARY KEY (year, pick),
                    UNIQUE (year, pick)
                )
                """,
                )
    _exec_command(pgc, commands)


def update_draft_tables(pgc : PgConnection):
    commands = (
                """
                ALTER TABLE player
                ADD draft_year INTEGER,
                ADD draft_team VARCHAR(255);

                UPDATE player
                    SET draft = b.pick,
                        draft_year = b.year,
                        draft_team = b.team
                FROM Draft AS b
                WHERE player_name = b.player AND player.link = b.link;
                """,
                )
    _exec_command(pgc, commands)


def create_draft_table(pgc : PgConnection):
    commands = (
                """
                CREATE TABLE draft(
                    year INTEGER NOT NULL,
                    pick INTEGER NOT NULL,
                    team VARCHAR(10) NOT NULL,
                    player VARCHAR(255) NOT NULL,
                    college VARCHAR(255),
                    link VARCHAR(255),
                    PRIMARY KEY (year, pick),
                    UNIQUE (year, pick)
                )
                """,
                )
    _exec_command(pgc, commands)


def create_player_tables(pgc : PgConnection):
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
    _exec_command(pgc, commands)


def _exec_command(pgc : PgConnection, commands : Tuple[str])-> None:
    conn = pgc.getConn()
    cur  = pgc.getCurs()
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
                'draft' : create_draft_table,
                'draft2player' : update_draft_tables,
                'box' : create_boxscore_tables}
    
    fx = fx_dict[create_type]
    fx(pgc)