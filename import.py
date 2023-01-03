from bs4 import BeautifulSoup
import requests 
import pandas as pd
import lxml

link = 'https://www.basketball-reference.com/leagues/NBA_2022_games.html'
data = requests.get(link).text
soup = BeautifulSoup(data, 'html.parser')
tables = soup.find_all('table', class_ = 'stats_table')
table = tables[0]
df = pd.read_html(str(table))
print(df)
