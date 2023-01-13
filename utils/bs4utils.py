import bs4
from bs4 import BeautifulSoup
import pandas as pd

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