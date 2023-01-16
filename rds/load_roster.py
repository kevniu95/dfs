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

# Schedule / Box_score
# game_id / season  /  tm1  /  tm2  /  stadium  /  statistics
# (create from pt of view of both teams later)

# box_score_player
# game_id  / player1 / opp  / statistics...
# game_id  / player2 / opp  / statistics...
    

def load_teams(bases : Dict[str, str], 
                rl : RequestLimiter, 
                trr : TeamRosterReader, 
                dao : Dfs_dao):
    """
    A. Get team links
    """
    team_links : Dict[str, str] = learn_teams_from_summary(bases['summary_base'], rl)
   
    """
    B. Load teams into DB
    """
    for tm, link in team_links.items():
        trr.set_team(tm)
        trr.set_link(link)
        
        stadium, player_table = trr.get_team_info() # This is where request is made
        df = trr.process_player_table(player_table)
        
        # 1. Create and load Team
        team_tup = [(YEAR, tm, stadium)]
        dao.team_to_db(team_tup)
        # 2. Create and load Player
        # player_tups = trr.process_rows_for_player(df)
        # dao.players_to_db(player_tups)
        # 3. Create and load Roster
        # roster_tups = trr.process_rows_for_roster(df, tm)
        # dao.roster_to_db(roster_tups)
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
                        limit = LIMIT - 1, 
                        load = LOAD_FILE)
    trr : TeamRosterReader = TeamRosterReader(None, None, YEAR, rl)
    dao : Dfs_dao = Dfs_dao(pgc)

    bases = {'summary_base' :BASE + f'/leagues/NBA_{YEAR}.html',
                'schedule_base' : BASE + '/leagues/NBA_%s_games-%s.html',
                'draft_base' : BASE + f'/draft/NBA_{YEAR}.html'}

    load_teams(bases = bases, rl = rl, trr = trr, dao = dao)