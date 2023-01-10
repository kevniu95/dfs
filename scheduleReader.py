
from bs4 import BeautifulSoup
from bs4.element import Tag
from requestLimiter import RequestLimiter
import pandas as pd
from bs4utils import get_ith_table, read_ith_table

def learn_schedule_from_month(link : str, rl : RequestLimiter) -> pd.DataFrame:
    data = rl.get(link)
    if not data:
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
    table['game_link'] = pd.Series(link_col)
    return table
    

class BoxscoreReader():
    def __init__(self, rl : RequestLimiter):
        self.rl = rl
        self.soup = None

    def set_link(self, link : str) -> None:
        self.link = link
    
    def get_soup(self) -> BeautifulSoup:
        if self.link:
            data = self.rl.get(self.link, waitForPop = True)
            soup : BeautifulSoup = BeautifulSoup(data.text, 'html.parser')
            self.soup = soup
            return soup
        else:
            print("No link has been set yet!")
    
    def __str__(self):
        return f"Link: {self.link}"
    
    def __repr__(self):
        return f"Link: {self.link}"