import pandas as pd
from bs4 import BeautifulSoup
import requests 
from typing import Dict

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

def main(year : int, bases : Dict[str, str]):
    teams : Dict[str, Dict] = learn_teams(bases['summary_base'])
    for k in teams.keys():
        arena, roster = get_team_info(k, teams)
        teams[k]['arena'] = arena
        teams[k]['roster'] = roster
    

def get_team_info(team_key : str, teams : Dict[str, str]):
    team = teams[team_key]
    tm_link = BASE = team['link']

    data = requests.get(tm_link).text
    soup = BeautifulSoup(data, 'html.parser')
    arena = get_arena(soup)
    roster = read_ith_table(soup, 0, id = 'roster')
    return arena, roster


def learn_teams(link : str) -> Dict[str, Dict]:
    tm_dict = {}
    data = requests.get(link).text
    soup = BeautifulSoup(data, 'html.parser')
    
    table = get_ith_table(soup, 4, class_ = 'stats_table')
    if table:
        rows = table.findChildren(['tr'])
        for row in rows:
            for a in row.find_all('a'):
                tm_dict[a.text] = {'link' : a.get('href')}
    else:
        print("Hit rate limit on website!")
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
    BASE = 'https://www.basketball-reference.com'
    bases = {'summary_base' :BASE + '/leagues/NBA_2023.html',
                    'schedule_base' : BASE + 'leagues/NBA_%s_games-%s.html'}
    
    year = 2022
    MONTHS = [i.lower() for i in ['October',
                'November',
                'December', 
                'January', 
                'February', 
                'March', 
                'April', 
                'May', 
                'June']]
    
    main(year, bases)