import sys
 
# adding Folder_2 to the system path
sys.path.insert(0, '../utils')

from typing import Dict
import argparse

from config import Config
from pgConnect import PgConnection
from dfs_dao import Dfs_dao
from requestLimiter import RequestLimiter
from teamRosterReader import TeamRosterReader, learn_teams_from_summary
from draftReader import DraftReader


def load_draft(start_year : int, 
                end_year : int, 
                dr : DraftReader,
                dao : Dfs_dao):
    for yr in range(start_year, end_year + 1):
        draft_tups = dr.process_rows_for_draft(yr, draft_base)
        dao.draft_to_db(draft_tups)


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
    parser.add_argument("st_year", help = "Starting year of draft data to be added ")
    parser.add_argument("end_year", help = "Ending year of draft data to be added ")
    args = parser.parse_args()
    ST_YEAR : int = int(args.st_year)
    END_YEAR: int = int(args.end_year)

    rl : RequestLimiter = RequestLimiter(BASE, 
                        interval = INTERVAL, 
                        limit = LIMIT - 1, 
                        load = LOAD_FILE)
    
    dr : DraftReader = DraftReader(rl)
    dao : Dfs_dao = Dfs_dao(pgc)

    draft_base = BASE + '/draft/NBA_{}.html'
    load_draft(ST_YEAR, END_YEAR, dr, dao)