import argparse
import datetime
from typing import Dict, List, Any
from bs4 import BeautifulSoup
from bs4utils import get_ith_table, read_ith_table
import pandas as pd


from config import Config
from pgConnect import PgConnection
from requestLimiter import RequestLimiter
from standingsReader import StandingsReader
from dfs_dao import Dfs_dao

season = {2023 : {'dates' : ['10-01-2022', '01-13-2023'],
                    'games' : None},
            2022 : {'dates' : ['10-01-2021', '07-01-2022'],
                    'games' : 82},
            2021 : {'dates' : ['12-01-2020', '08-01-2021'],
                    'games' : 72},
            2020 : {'dates' : ['10-01-2019', '10-31-2020'],
                    'games' : 72},
            2019 : {'dates' : ['10-01-2018', '07-01-2019'],
                    'games' : 82},
            2018 : {'dates' : ['10-01-2017', '07-01-2018'],
                    'games' : 82}
        }



teams = ['Charlotte Hornets',
        'Philadelphia 76ers',
        'Washington Wizards',
        'Chicago Bulls',
        'Cleveland Cavaliers',
        'Sacramento Kings',
        'New Orleans Pelicans',
        'Milwaukee Bucks',
        'Miami Heat',
        'Brooklyn Nets',
        'Indiana Pacers',
        'Golden State Warriors',
        'Toronto Raptors',
        'Atlanta Hawks',
        'Oklahoma City Thunder',
        'Los Angeles Lakers',
        'Boston Celtics',
        'Houston Rockets',
        'Dallas Mavericks',
        'New York Knicks',
        'Memphis Grizzlies',
        'Utah Jazz',
        'Denver Nuggets',
        'San Antonio Spurs',
        'Minnesota Timberwolves',
        'Portland Trail Blazers',
        'Detroit Pistons',
        'Los Angeles Clippers',
        'Phoenix Suns',
        'Orlando Magic']


"https://www.basketball-reference.com/leagues/NBA_2019_standings.html"

def validate_db_games(years : Dict[str, Dict[str, Any]], sr : StandingsReader):
    for season, months in list(years.items())[:1]:
        std_base : str = BASE + f'/leagues/NBA_{season}_standings.html'
        sr.set_link(std_base)
        soup : BeautifulSoup = sr.get_soup()

        real_games : Dict[str, int] = sr.get_real_tm_games(soup)
        for tm, games in real_games.items():
            print(tm, games)
            # Use DAO to query database given month range

    


        

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
    parser.add_argument("--year", help = "Year of data to be added", nargs = '?')
    
    args = parser.parse_args()
    
    if args.year:
        YEAR : int = int(args.year)
    
    rl : RequestLimiter = RequestLimiter(BASE, 
                        interval = INTERVAL, 
                        limit = LIMIT - 1, 
                        load = LOAD_FILE)
    stRead : StandingsReader = StandingsReader(rl)
    
    dao : Dfs_dao = Dfs_dao(pgc)

    schedule_base = BASE + '/leagues/NBA_{}_games-{}.html'
    
    validate_db_games(season, stRead)