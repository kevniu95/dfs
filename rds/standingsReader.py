from typing import Tuple, List, Any, Dict
from bs4 import BeautifulSoup
from bs4.element import Tag
from bs4utils import get_ith_table, read_ith_table
import pandas as pd
import numpy as np

from requestLimiter import RequestLimiter

class StandingsReader():
    """
    A. Constructor
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
    
            
    def get_real_tm_games(self, soup : BeautifulSoup):
        tm_games : Dict[str, int] = {}

        for i in ['E', 'W']:
            tab = read_ith_table(soup, 0, id = f'confs_standings_{i}')
            tab.iloc[:, 0] = tab.iloc[:, 0].str.replace(r"\(.*\)","", regex = True).str.strip().str.replace('*','', regex = False)
            for _, row in tab.iterrows():
                tm_games[row[0]] = row[1] + row[2]
        return tm_games