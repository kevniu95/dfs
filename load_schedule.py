import sys
 
# adding Folder_2 to the system path
sys.path.insert(0, '../utils')

from typing import Dict, List, Tuple
from configparser import ConfigParser
import argparse
import requests 

from bs4 import BeautifulSoup
import bs4
import pandas as pd
from pgConnect import PgConnection
from requestLimiter import RequestLimiter
from limitedScraper import LimitedScraper
from config import Config
from teamRosterReader import TeamRosterReader, learn_teams_from_summary
from dfs_dao import Dfs_dao
from bs4utils import read_ith_table, get_ith_table

# Schedule / Box_score
# game_id / season  /  tm1  /  tm2  /  stadium  /  statistics
# (create from pt of view of both teams later)

# box_score_player
# game_id  / player1 / opp  / statistics...
# game_id  / player2 / opp  / statistics...


"""
1. Things to DB
"""

'''
Load teams function
'''
def load_teams(year : int, bases : Dict[str, str], rl : RequestLimiter, trr : TeamRosterReader, dao : Dfs_dao):
    team_links : Dict[str, str] = learn_teams_from_summary(bases['summary_base'], rl)
    tl = dict((k, team_links[k]) for k in ['Boston Celtics', 'Dallas Mavericks', 'Phoenix Suns'])
    for tm, link in tl.items():
        trr.set_team(tm)
        trr.set_link(link)
        stadium, player_table = trr.get_team_info()

        team_tup = [(YEAR, tm, stadium)]
        dao.team_to_db(team_tup)

        df = trr.process_player_table(player_table)
        player_tups = trr.process_rows_for_player(df)
        dao.players_to_db(player_tups)
        
        roster_tups = trr.process_rows_for_roster(df, tm)
        dao.roster_to_db(roster_tups)

    return


if __name__ == '__main__':
    # ======
    # 1. Read configs
    # ======
    config : Config = Config()
    pgc : PgConnection = PgConnection(config)
    
    # reader 
    read_constants : Dict[str, str] = config.parse_section('reader')
    BASE : str = read_constants['base']
    NAME : str = BASE[BASE.find('.') + 1:]

    # requestLimiter
    rl_constants : Dict[str, str] = config.parse_section('requestLimiter')
    load_loc = rl_constants['load_location']
    LOAD_FILE : str = f'{load_loc}{NAME}.p'
    INTERVAL : int = int(rl_constants['interval'])
    LIMIT : int = int(rl_constants['limit'])
    
    # ======
    # 2. Parse args
    # ======
    parser = argparse.ArgumentParser()
    parser.add_argument("year", help = "Year of data to be added ")
    args = parser.parse_args()
    YEAR : int = int(args.year)
    
    rl : RequestLimiter = RequestLimiter(BASE, 
                        interval = INTERVAL, 
                        limit = LIMIT, 
                        load = LOAD_FILE)
    
    trr : TeamRosterReader = TeamRosterReader(None, None, YEAR, rl)
    dao : Dfs_dao = Dfs_dao(pgc)

    bases = {'summary_base' :BASE + f'/leagues/NBA_{YEAR}.html',
                'schedule_base' : BASE + '/leagues/NBA_%s_games-%s.html'}

    load_teams(year = YEAR, bases = bases, rl = rl, trr = trr, dao = dao)

    # MONTHS : List[int] = [i.lower() for i in ['October',
    #         'November',
    #         'December', 
    #         'January', 
    #         'February', 
    #         'March', 
    #         'April', 
    #         'May', 
    #         'June']]


# def load_month(month):
#     link = base % (2022, month)
#     df = read_ith_table(link)
#     print(df)
