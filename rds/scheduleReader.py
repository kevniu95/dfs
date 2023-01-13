from typing import Tuple, List, Any
from bs4 import BeautifulSoup
from bs4.element import Tag
from bs4utils import get_ith_table, read_ith_table
import pandas as pd
import numpy as np

from requestLimiter import RequestLimiter

def learn_schedule_from_month(link : str, rl : RequestLimiter) -> pd.DataFrame:
    print(link)
    try:
        data = rl.get(link, waitForPop = True)
    except:
        print("This month doesn't exist while trying learn_schedule_from_month() function!")
        return 
    else:
        if not data or not data.ok:
            print("Couldn't get information in learn_schedule_from_month() function!")
            return 
    data = data.text
    soup : BeautifulSoup = BeautifulSoup(data, 'html.parser')
    
    html_table : Tag = get_ith_table(soup, 0, id='schedule')
    link_col = []
    if html_table:
        rows = html_table.findChildren(['tr'])
        for row in rows:
            for a in row.find_all('a'):
                if a.text.find('Box Score') > -1:
                    link_col.append(a.get('href'))
        
    table : pd.DataFrame = read_ith_table(soup, 0, id='schedule')
    if table is None:
        print("No table found so moving to another month...")
        return 
    table['game_link'] = pd.Series(link_col, dtype = 'object')
    table = table[table['game_link'].notnull()]
    return table
    

class BoxscoreReader():
    """
    A. Constructor and Setter
    """
    def __init__(self, rl : RequestLimiter):
        self.rl = rl
        self.soup = None

    def set_link(self, link : str) -> None:
        self.link = link
    
    """
    B. Public Methods
    """
    def get_soup(self) -> BeautifulSoup:
        if self.link:
            data = self.rl.get(self.link, waitForPop = True)
            soup : BeautifulSoup = BeautifulSoup(data.text, 'html.parser')
            self.soup = soup
            return soup
        else:
            print("No link has been set yet!")
    

    def get_all_info(self, soup : BeautifulSoup):
        """"
        Returns
            Tuple with 4 items
                - tm1_tuple, tm1_player_list (tuples)
                - tm2_tuple, tm2_player_list (tuples)
        """

        # A. Get teams
        tm1, tm2 = self._get_team_names(soup)

        tabs = len(soup.find_all('div', class_ = 'filter switcher')[0].find_all('div'))
        tm1_inds = [0, tabs] 
        tm2_inds = [tabs + 1, (tabs + 1) * 2 - 1]
        
        # B. Generate original team and player tuples
        tm1_tup, tm1_players = self._process_team_tables(soup, tm1_inds)
        tm2_tup, tm2_players = self._process_team_tables(soup, tm2_inds)
        
        # C. Edit teams before returning
        tm1_players = self._check_players(tm1_players)
        tm2_players = self._check_players(tm2_players)

        tm1_tup = (tm1, '0') + tm1_tup # Extra 0/1 represents away vs home
        tm2_tup = (tm2, '1') + tm2_tup

        return tm1_tup, tm1_players, tm2_tup, tm2_players

    
    def process_time(self, tim : str) -> str:
            print(f"Here is the tim: {tim}")
            tim_list = tim.split(':')
            
            tim_list[1] = tim_list[1][:-1]
            if 'p' in tim and tim_list[0] != '12':
                tim_list[0] = int(tim_list[0]) + 12
            if 'a' in tim and tim_list[0] == '12':
                tim_list[0] = '00'
            tim_list.append('00')
            tim_str = ':'.join([str(i) for i in tim_list])
            return tim_str


    def update_player_tups(self,
                            game_info : Tuple[Any,...], 
                            player_tups : List[Tuple[Any,...]], 
                            tm1 : str, 
                            tm2 : str) -> List[Tuple[Any,...]]:
        for i in range(len(player_tups)):
            res = game_info + (tm1, tm2) + player_tups[i]
            player_tups[i] = res
        return player_tups


    """
    C. Primary Helpers
    """
    def _get_team_names(self, soup : BeautifulSoup) -> Tuple[str, str]:
        tm1 : str = soup.find_all('strong')[1].text.strip()
        tm2 : str = soup.find_all('strong')[2].text.strip()
        return tm1, tm2
    
    def _process_team_tables(self, soup : BeautifulSoup, inds : List[int]):
        tmTup : Tuple[Any, ...] = ()
        playerTups : List[Tuple[Any, ...]] = []
        for ind in inds:
            df = read_ith_table(soup, ind)
            df.columns = df.columns.droplevel()
            tmTup += self._get_tm_info(df)
            playerTups = self._get_player_tuples(df, playerTups)
        return tmTup, playerTups
    
    def _check_players(self, players : List[Tuple[Any,...]]) -> List[Tuple[Any,...]]:
        """
        Returns
            New List of tuples
            -Removes player name and MP after asserting basic stats match advacned
        """
        new_players = []
        for p in players:
            p = list(p)
            assert p[0] == p[21]
            p.pop(21)
            p.pop(21)
            new_players.append(tuple(p))
        return new_players

    """
    D. Auxiliary Helpers
    """
    def _get_tm_info(self, df : pd.DataFrame) -> Tuple[Any, ...]:
        last_row = df.iloc[-1, :]
        assert last_row['Starters'] == 'Team Totals'
        curr_list = list(last_row)[2:]
        while curr_list[-1] in [np.nan, None]:
            curr_list.pop(-1)
        tup = tuple(curr_list)
        return tup
    
    def _get_player_tuples(self, df : pd.DataFrame(), playerTups : List[Tuple[Any,...]]) -> List[Tuple[Any, ...]]:
        player_df = df[df['MP'].str.find(':') > -1].copy()
        player_df = player_df.sort_values(['MP', 'Starters'], ascending = [False, True]).reset_index()
        if len(player_df.columns == 17) and list(player_df.columns)[-1] == 'DRtg':
            player_df['dummy'] = np.nan
        for num, row in player_df.iterrows():
            player_list : List[Any] = list(row)[1:]
            player_list[1] = self._get_minutes(player_list[1])
            player_tup = tuple(player_list)
            if len(playerTups) <= num:
                playerTups.append(player_tup)
            else:
                playerTups[num] = playerTups[num] + player_tup    
        return playerTups
    
    def _get_minutes(self, mins : str) -> float:
        mins, seconds = mins.split(':')
        return str(round(float(mins) + float(seconds) / 60,2))

    
    def __str__(self):
        return f"Link: {self.link}"
    
    def __repr__(self):
        return f"Link: {self.link}"