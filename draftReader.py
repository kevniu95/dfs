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

class DraftReader():
    def __init__(self, rl : RequestLimiter):
        self.rl : RequestLimiter = rl
        

    def process_rows_for_draft(self, year : int, link_base : str) -> List[Tuple[Any, ...]]:
        link = link_base.format(year)
        table : Tag = self._getHtmlTable(link)
        df : pd.DataFrame = self._process_draft_table(table)
        rows : List[Tuple[Any, ...]] = self._process_rows_for_draft(df, year)
        return rows
        

    def _getHtmlTable(self, link : str) -> Tag:
        res = self.rl.get(link, waitForPop = True)
        data = res.text
        soup : BeautifulSoup = BeautifulSoup(data, 'html.parser')
        soup
        table : Tag = get_ith_table(soup, 0, id = 'stats')
        return table


    def _process_draft_table(self, table : Tag) -> pd.DataFrame:
        thead = table.find('thead')
        data = []
        column_names = [th.text.strip() for th in thead.find_all('th')]
        column_names = column_names[column_names.index('Advanced') + 1:]
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
        data
        had_link = list(had_link)
        had_link.sort()
        for val in had_link:
            column_names.append(column_names[val] + '_link')

        df = pd.DataFrame(data[2:], columns= column_names)
        my_res = df[['Pk', 'Tm', 'Player', 'College', 'Player_link']]
        return my_res
    
    
    def _process_rows_for_draft(self, df : pd.DataFrame, year: int) -> List[Tuple[Any, ...]]:
        rows = []
        for _, row in df.iterrows():
            if row['Pk'] not in (None, 'Pk'):
                out = (year,
                        row['Pk'],
                        row['Tm'],
                        row['Player'],
                        row['College'],
                        row['Player_link'])
                rows.append(out)
        return rows