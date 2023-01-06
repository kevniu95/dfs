from typing import Dict, List, Tuple
from configparser import ConfigParser
import argparse
import requests 

from bs4 import BeautifulSoup
import bs4
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


"""
1. Things to DB
"""
def players_to_db(tups):
    args = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s)", i).decode('utf-8') for i in tups)
    qry = "INSERT INTO player VALUES " + (args) + " ON CONFLICT (player_name, dob, height, weight) DO NOTHING"
    # print(qry)
    try:
        cur.execute(qry)
        conn.commit()
        print("Commited player insertion!")
    except Exception as e:
        print("Couldn't execute and commit player insertion!")
        print(str(e))

def roster_to_db(tups):
    args = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s)", i).decode('utf-8') for i in tups)
    qry = "INSERT INTO roster VALUES " + (args) + " ON CONFLICT (season, team, player_name, dob, height, weight) DO NOTHING"
    try:
        cur.execute(qry)
        conn.commit()
        print("Commited roster insertion!")
    except Exception as e:
        print("Couldn't execute and commit roster insertion!")
        print(str(e))

def team_to_db(team_tup):
    args = ','.join(cur.mogrify("(%s,%s,%s)", i).decode('utf-8') for i in team_tup)
    qry = "INSERT INTO team VALUES " + (args) + " ON CONFLICT (season, team) DO NOTHING"
    try:
        cur.execute(qry)
        conn.commit()
        print("Commited team insertion!")
    except Exception as e:
        print("Couldn't execute and commit team insertion!")
        print(str(e))


"""
Prepare tuples
"""
def process_player_table(player_table):
    table = player_table
    thead = player_table.find('thead')
    data = []
    column_names = [th.text.strip() for th in thead.find_all('th')]
    had_link = set()
    for row in table.find_all(['tr']):
        row_data = []
        link_data = []
        for num, td in enumerate(row.find_all(['td','th'])):
            not_link = None
            if ''.join(td.text.strip()):
                not_link = ''.join(td.text.strip())
            row_data.append(not_link)

            if td.find('a'):
                link = td.a['href']
                link_data.append(link)
                had_link.add(num)
        set_cols = True
        data.append(row_data + link_data)

    had_link = list(had_link)
    had_link.sort()
    for val in had_link:
       column_names.append(column_names[val] + '_link')

    df = pd.DataFrame(data[1:], columns= column_names)
    return df


def process_rows_for_player(df):
    rows = []
    for num, row in df.iterrows():
        out = (process_name(row['Player']),
                row['Birth Date'],
                process_height(row['Ht']),
                int(row['Wt']),
                None,
                process_debut_season(row['Exp']),
                row[''].upper(),
                row['College'],
                row['Player_link'])
        rows.append(out)
    return rows

def process_rows_for_roster(df, tm):
    rows = []
    for num, row in df.iterrows():
        out = (YEAR,
                tm,
                process_name(row['Player']),
                row['Birth Date'],
                process_height(row['Ht']),
                row['Wt'],
                row['No.'],
                row['Pos'])
        rows.append(out)
    return rows

# Tuple helpers
def process_debut_season(exp : str) -> int:
    num = int(exp.replace('R','0'))
    return YEAR - num

def process_height(ht : str) -> int:
    ht_split = ht.split('-')
    ft, inch = ht_split[0], ht_split[1]
    return int(ft) * 12 + int(inch)

def process_name(name : str) -> str:
    if name[-4:] == '(TW)':
        return name[:-4].strip()
    return name.strip()


'''
Load teams function
'''
def load_teams(year : int, bases : Dict[str, str], rl : RequestLimiter):
    team_links : Dict[str, str] = learn_teams(bases['summary_base'], rl)
    tl = dict((k, team_links[k]) for k in ['Boston Celtics','Miami Heat','Detroit Pistons'])
    for tm, link in tl.items():
        (stadium, player_table) : Tuple(str, pd.DataFrame) = get_team_info(tm, tl[tm], rl)

        team_tup = [(YEAR, tm, stadium)]
        team_to_db(team_tup)

        df = process_player_table(player_table)
        player_tups = process_rows_for_player(df)
        players_to_db(player_tups)
        
        roster_tups = process_rows_for_roster(df, tm)
        roster_to_db(roster_tups)

    return


def get_team_info(team : str, link : str, rl : RequestLimiter) -> Tuple(str, pd.DataFrame):
    data =rl.get(requests.get, link)
    if not data:
        print(f"Unable to retrieve team info for {team}!")
        return
    soup : BeautifulSoup = BeautifulSoup(data.text, 'html.parser')
    arena : str = get_arena(soup)
    roster : pd.DataFrame = read_ith_table(soup, 0, id = 'roster')
    return arena, roster

def get_arena(soup : BeautifulSoup) -> str:
    # Find arena
    a = soup.find_all('div', id = 'meta')[0]
    p = a.find_all('p')[-1]
    arena = p.contents[2].strip()
    return arena


def learn_teams(link : str, rl : RequestLimiter) -> Dict[str, str]:
    tm_dict = {}
    data = rl.get(requests.get, link)
    if not data:
        print("Couldn't get information in learn_teams() function!")
        return 
    data = data.text
    soup : BeautifulSoup = BeautifulSoup(data, 'html.parser')
    
    table : bs4.element.Tag = get_ith_table(soup, 4, class_ = 'stats_table')
    if table:
        rows = table.findChildren(['tr'])
        for row in rows:
            for a in row.find_all('a'):
                tm_dict[a.text] = BASE + a.get('href')
    else:
        print("Previously hit rate limit on website!")
    return tm_dict


def get_ith_table(soup : BeautifulSoup, i : int, **kwargs) -> bs4.element.Tag:
    # Get and return table
    tables = soup.find_all('table', **kwargs)
    if len(tables) > 0:
        table = tables[i]
        return table
    else:
        print("No table found on this HTML page!")
        

def read_ith_table(soup : BeautifulSoup, i : int, **kwargs) -> pd.DataFrame:
    table : bs4.element.tag = get_ith_table(soup, i, **kwargs)
    if table:
        return pd.read_html(str(table), flavor='html5lib')[0]    

    

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


# def load_month(month):
#     link = base % (2022, month)
#     df = read_ith_table(link)
#     print(df)
