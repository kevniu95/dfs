import argparse
from typing import Dict, Any
from bs4 import BeautifulSoup

from config import Config
from pgConnect import PgConnection
from requestLimiter import RequestLimiter
from standingsReader import StandingsReader
from dfs_dao import Dfs_dao

season_info = { 2023 : {'dates' : ['10-01-2022', '01-15-2023']},
            2022 : {'dates' : ['10-01-2021', '04-11-2022']},
            2021 : {'dates' : ['12-01-2020', '05-17-2021']},
            2020 : {'dates' : ['10-01-2019', '8-14-2020']},
            2019 : {'dates' : ['10-01-2018', '04-12-2019']},
            2018 : {'dates' : ['10-01-2017', '04-13-2018']},
        }


def validate_db_games(years : Dict[str, Dict[str, Any]], sr : StandingsReader, dao : Dfs_dao):
    print("\nChecking number of games in database match games in standings...")
    for season, months in list(years.items()):
        print(season)
        std_base : str = BASE + f'/leagues/NBA_{season}_standings.html'
        sr.set_link(std_base)
        soup : BeautifulSoup = sr.get_soup()

        # Get game number from team summary
        real_games : Dict[str, int] = sr.get_real_tm_games(soup)
        # Get game number from DB
        dates = season_info[season]['dates']
        db_t_games : Dict[str, int] = dao.get_team_game_num(dates[0], dates[1])
        db_p_games : Dict[str, int] = dao.get_team_game_num_fp(dates[0], dates[1])

        for k, v in real_games.items():
            if db_t_games[k] != real_games[k]:
                print(k, v)
            assert db_t_games[k] == real_games[k]

            if db_p_games[k] != real_games[k]:
                print(k,v)
            assert db_p_games[k] == real_games[k]
    print("Done.")

    print("\nChecking player and team box score observations are same...")
    dao.validate_same_box_games()
    print("Done.")

    print("\nChecking that box scores are internally consistent in database...")
    dao.validate_internal_box_score_consistency()
    print("Done.")


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
    
    validate_db_games(season_info, stRead, dao)