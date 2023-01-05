from typing import Dict, List
from configparser import ConfigParser
import argparse
import requests 

from bs4 import BeautifulSoup
import pandas as pd
from requestLimiter import RequestLimiter
from limitedScraper import LimitedScraper
from config import Config

# Season
# season  /  team_name  /  stadium_name

# Rosters 
# season / team_name / player_id / player_name / Pos / Exp

# Players
# player_id / player_name / birth_date / country / start_yr / college

# Schedule / Box_score
# game_id / season  /  tm1  /  tm2  /  stadium  /  statistics
# (create from pt of view of both teams later)

# box_score_player
# game_id  / player1 / opp  / statistics...
# game_id  / player2 / opp  / statistics...


# Teams
# id / teamName /  
def test():
    pass

def load_teams(year : int, bases : Dict[str, str], rl : RequestLimiter):
    team_links : Dict[str, str] = learn_teams(bases['summary_base'], rl)
    tl = dict((k, team_links[k]) for k in ['Boston Celtics'])
    print(tl)
    dat = get_team_info('Boston Celtics', tl['Boston Celtics'], rl)
    print(dat)
    # fx = test    
    # ls : LimitedScraper = LimitedScraper(fx = fx,
    #                                         name = 'team_info',
    #                                         linkDict = team_links,
    #                                         rl = rl)
    # teams : Dict[str, dict]
    # for team, link in team_links.items():
    #     get_team_info(team, link, rl)
    #     teams[k]['arena'] = arena
    #     teams[k]['roster'] = roster


def get_team_info(team : str, link : str, rl : RequestLimiter):
    data =rl.get(requests.get, link)
    if not data:
        print(f"Unable to retrieve team info for {team}!")
        return
    soup = BeautifulSoup(data.text, 'html.parser')
    arena = get_arena(soup)
    roster = read_ith_table(soup, 0, id = 'roster')
    roster_table = get_ith_table(soup, 0, id = 'roster')
    return arena, roster


def learn_teams(link : str, rl : RequestLimiter) -> Dict[str, str]:
    tm_dict = {}
    data = rl.get(requests.get, link)
    if not data:
        print("Couldn't get information in learn_teams() function!")
        return 
    data = data.text
    soup = BeautifulSoup(data, 'html.parser')
    
    table = get_ith_table(soup, 4, class_ = 'stats_table')
    if table:
        rows = table.findChildren(['tr'])
        for row in rows:
            for a in row.find_all('a'):
                tm_dict[a.text] = BASE + a.get('href')
    else:
        print("Previously hit rate limit on website!")
    return tm_dict

        
def get_arena(soup):
    # Find arena
    a = soup.find_all('div', id = 'meta')[0]
    p = a.find_all('p')[-1]
    arena = p.contents[2].strip()
    return arena


def get_ith_table(soup, i, **kwargs):
    # Get and return table
    tables = soup.find_all('table', **kwargs)
    if len(tables) > 0:
        table = tables[i]
        return table
    else:
        print("No table found on this HTML page!")
        
def read_ith_table(soup, i, **kwargs):
    table = get_ith_table(soup, i, **kwargs)
    if table:
        return pd.read_html(str(table), flavor='html5lib')[0]    

def load_month(month):
    link = base % (2022, month)
    df = read_ith_table(link)
    print(df)
    

if __name__ == '__main__':
    # ======
    # 1. Read configs
    # ======
    config : Config = Config()
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
    bases = {'summary_base' :BASE + f'/leagues/NBA_{YEAR}.html',
                'schedule_base' : BASE + '/leagues/NBA_%s_games-%s.html'}
    load_teams(year = YEAR, bases = bases, rl = rl)

    # MONTHS : List[int] = [i.lower() for i in ['October',
    #         'November',
    #         'December', 
    #         'January', 
    #         'February', 
    #         'March', 
    #         'April', 
    #         'May', 
    #         'June']]
