import sys
 # adding Folder_2 to the system path
sys.path.insert(0, '../utils')

import re
from typing import Dict, List, Tuple, Any
import argparse
import pandas as pd

from config import Config
from pgConnect import PgConnection
from dfs_dao import Dfs_dao
from requestLimiter import RequestLimiter
from scheduleReader import BoxscoreReader, learn_schedule_from_month
from bs4 import BeautifulSoup

def load_schedule(schedule_base : str, 
                    rl : RequestLimiter,
                    br : BoxscoreReader):
    
    for month in MONTHS:
        print('\n' + month)
        link : str = schedule_base.format(YEAR, month)
        df : pd.DataFrame = learn_schedule_from_month(link, rl)
        if df is None or len(df) == 0:
            print("Continuing to next month...")
            continue
        
        ctr = 0
        for _, row in df.iterrows():
            link : str = BASE + row['game_link']
            print(link)
            br.set_link(link)
            
            soup : BeautifulSoup  = br.get_soup()
            tm1_tuple, tm1_players, tm2_tuple,tm2_players = br.get_all_info(soup)
            
            game_info = (row['Date'], br.process_time(row['Start (ET)']), treat_attend(row['Attend.']), row['Arena']) 
            game_entry_tuple1 = game_info + tm1_tuple + tm2_tuple
            game_entry_tuple2 = game_info + tm2_tuple + tm1_tuple

            tm1 : str = game_entry_tuple1[4]
            tm2 : str = game_entry_tuple2[4]

            tm1_players : List[Tuple[Any, ...]] = br.update_player_tups(game_info[:2], tm1_players, tm1, tm2)
            tm2_players : List[Tuple[Any, ...]] = br.update_player_tups(game_info[:2], tm2_players, tm2, tm1)
            
            dao.team_box_to_db([game_entry_tuple1, game_entry_tuple2])
            dao.player_box_to_db(tm1_players)
            dao.player_box_to_db(tm2_players)
            print()

            # ctr += 1
            # if ctr > 0:
            #     break
            
def treat_attend(attd : int):
    attd = re.sub(r"\.0$","", str(attd))
    if len(attd) == 0 or attd == 'nan':
        return '0'
    return attd


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
                        limit = LIMIT - 1, 
                        load = LOAD_FILE)
    br : BoxscoreReader = BoxscoreReader(rl)
    dao : Dfs_dao = Dfs_dao(pgc)

    schedule_base = BASE + '/leagues/NBA_{}_games-{}.html'
    
    MONTHS : List[str] = ['october', 
                            'november', 
                            'december', 
                            'january',
                            'february',
                            'march',
                            'april',
                            'may',
                            'june']
    
    load_schedule(schedule_base, rl, br)
    