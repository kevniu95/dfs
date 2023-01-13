import requests
from typing import Dict, Tuple, List, Any
from bs4 import BeautifulSoup
from bs4.element import Tag
import pandas as pd

from bs4utils import get_ith_table
from config import Config
from requestLimiter import RequestLimiter


config : Config = Config()
# reader 
read_constants : Dict[str, str] = config.parse_section('reader')
BASE : str = read_constants['base']


"""
A. Separate, independent function
    - Gets team names and links from {YEAR}_Summary page
"""
def learn_teams_from_summary(link : str, rl : RequestLimiter) -> Dict[str, str]:
    tm_dict = {}
    data = rl.get(link)
    if not data:
        print("Couldn't get information in learn_teams_from_summary() function!")
        return 
    data = data.text
    soup : BeautifulSoup = BeautifulSoup(data, 'html.parser')
    
    table : Tag = get_ith_table(soup, 4, class_ = 'stats_table')
    if table:
        rows = table.findChildren(['tr'])
        for row in rows:
            for a in row.find_all('a'):
                tm_dict[a.text] = BASE + a.get('href')
    else:
        print("Previously hit rate limit on website!")
    return tm_dict


class TeamRosterReader():
    """
    Functions to read from a single Team page
    (e.g., https://www.basketball-reference.com/teams/BOS/2023.html)
    """
    def __init__(self, tm : str, link : str, year : int, rl : RequestLimiter):
        self.tm : str = tm
        self.link : str= link
        self.year : int = year
        self.rl : RequestLimiter = rl

    """
    Getters and setters
        -Do for each (tm, link) pair
    """
    def set_team(self, tm : str) -> None:
        self.tm = tm


    def set_link(self, link : str) -> None:
        self.link = link


    """
    B. Get team info
    """
    def get_team_info(self) -> Tuple[str, Tag]:
        data = self.rl.get(self.link, waitForPop = True)
        if not data:
            print(f"Unable to retrieve team info for {self.tm}!")
            return
        soup : BeautifulSoup = BeautifulSoup(data.text, 'html.parser')
        arena : str = self.get_arena(soup)
        player_table : Tag = get_ith_table(soup, 0, id = 'roster')
        return arena, player_table


    def get_arena(self, soup : BeautifulSoup) -> str:
        a = soup.find_all('div', id = 'meta')[0]
        p = a.find_all('p')[-1]
        if "Playoffs" in p.text:
            p = a.find_all('p')[-2]
        arena = p.contents[2].strip()
        return arena

    """
    C. Get player info
    """
    def process_player_table(self, table : Tag) -> pd.DataFrame:
        """
        Takes HTML table and adds links before creating pd.DataFrame
        """
        table = table
        thead = table.find('thead')
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
            data.append(row_data + link_data)

        had_link = list(had_link)
        had_link.sort()
        for val in had_link:
            column_names.append(column_names[val] + '_link')

        df = pd.DataFrame(data[1:], columns= column_names)
        return df


    def process_rows_for_player(self, df : pd.DataFrame) -> List[Tuple[Any, ...]]:
        rows = []
        for num, row in df.iterrows():
            out = (self._process_name(row['Player']),
                    row['Birth Date'],
                    self._process_height(row['Ht']),
                    int(row['Wt']),
                    None,
                    self._process_debut_season(row['Exp']),
                    row[''].upper(),
                    row['College'],
                    row['Player_link'])
            rows.append(out)
        return rows


    def process_rows_for_roster(self, df : pd.DataFrame, tm : str):
        rows = []
        for _, row in df.iterrows():
            out = (self.year,
                    tm,
                    self._process_name(row['Player']),
                    row['Birth Date'],
                    self._process_height(row['Ht']),
                    row['Wt'],
                    self._process_number(row['No.']),
                    row['Pos'])
            rows.append(out)
        return rows


    # Tuple helpers
    def _process_number(self, num : str) -> int:
        ans = None
        try:
            ans =  int(num[:num.find(',')])
        except:
            pass
        return ans


    def _process_debut_season(self, exp : str) -> int:
        num = int(exp.replace('R','0'))
        return self.year - num

    def _process_height(self, ht : str) -> int:
        ht_split = ht.split('-')
        ft, inch = ht_split[0], ht_split[1]
        return int(ft) * 12 + int(inch)

    def _process_name(self, name : str) -> str:
        if name[-4:] == '(TW)':
            return name[:-4].strip()
        return name.strip()