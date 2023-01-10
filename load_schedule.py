import sys
 
# adding Folder_2 to the system path
sys.path.insert(0, '../utils')

from typing import Dict, List
import argparse

from config import Config
from pgConnect import PgConnection
from dfs_dao import Dfs_dao
from requestLimiter import RequestLimiter
from teamRosterReader import TeamRosterReader, learn_teams_from_summary
from scheduleReader import BoxscoreReader, learn_schedule_from_month
from bs4 import BeautifulSoup

def load_schedule(schedule_base : str, 
                    rl : RequestLimiter,
                    br : BoxscoreReader):
    for month in MONTHS[:1]:
        link = schedule_base.format(YEAR, month)
        df = learn_schedule_from_month(link, rl)
        
        ctr = 0
        for num, row in df.iterrows():
            link = BASE + row['game_link']
            br.set_link(link)
            soup : BeautifulSoup  = br.get_soup()
            
            emptyTuple = ()
            # br.get_line_score(emptyTuple)
            # br.get_four_factors()
            # br.get_box_1()
            # br.get_box_2()
            # br.get_abox_1()
            # br.get_abox_2()

            # player_box, tm_box = process_df_for_tups()

            # dao.player_box_to_db()
            # dao.tm_box_to_db()
            
            ctr += 1
            if ctr > 0:
                break
            

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
    trr : TeamRosterReader = TeamRosterReader(None, None, YEAR, rl)
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
    