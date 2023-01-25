import requests
import os
from typing import List, Any, Tuple
from datetime import datetime
import re
import csv
import argparse

from bs4 import BeautifulSoup
from bs4.element import Tag, ResultSet
import load_injury_info
import boto3
from botocore.exceptions import ClientError

from config import Config
from pgConnect import PgConnection
from dfs_dao import Dfs_dao


link = 'https://rotogrinders.com/lineups/nba?date=2023-01-22&site=fanduel'
a = requests.get(link)

def process_date(link : str) -> datetime:
    l = link.find('date=') + 5
    r = link.rfind('&')
    txt = link[l:r]
    date = datetime.strptime(txt, '%Y-%m-%d')
    return date

def get_games(link : str):
    a = requests.get(link)
    tables = BeautifulSoup(a.text, features = 'html5lib').find_all('div', 
                                            class_ = 'blk crd lineup')
    return tables

def get_csv_tuples_from_game(game : Tag, dt : datetime) -> List[List[Any]]:
    time : str = game.find_all('time')
    teams : Tag = game.find_all('div', class_ = 'teams')[0]
    
    # Get team names
    short_names : ResultSet = teams.find_all('span', class_ = 'shrt')
    away_team, home_team = (short_name.text for short_name in short_names)
    
    # Get game time, and final datetime
    game_time_tag : Tag = game.find_all('div', class_ = 'weather-status')[0]
    game_time : str = game_time_tag.text.strip()
    
    clean_time_str : str = process_time(game_time[:-4].replace(' ',''))
    final_dt = datetime.combine(dt.date(), 
                                datetime.strptime(clean_time_str,'%H:%M:%S').time())
    
    # Away tm, home tm, date, time, player_tm, ....
    away_players = get_players(game, 'away', away_team)
    home_players = get_players(game, 'home', home_team)
    all_players = away_players + home_players

    for num, player in enumerate(all_players):
        all_players[num] = [away_team, home_team, final_dt, dt, game_time] + player
    
    return all_players

def get_players(game : Tag, whichTeam : str, tm_name : str):
    tm = game.find_all('div', class_ = f'blk {whichTeam}-team')[0]
    
    player_section = tm.find_all('ul', class_ = re.compile(r'players nba'))[0]
    player_tags = player_section.find_all('li', class_ = 'player')
    
    tuple_ends : List[List[str]] = []
    for player_tag in player_tags:
        txt : str = player_tag.text
        txt = re.sub(r'[\n ]{2,}', ',', txt.strip())
        txt_list = txt.split(',')
        if len(txt_list) >= 4:
            end = [tm_name, txt_list[0], txt_list[1], txt_list[3]]
            tuple_ends.append(end)
    return tuple_ends

def process_time(tim : str) -> str:
    tim_list = tim.split(':')
    
    tim_list[1] = tim_list[1][:-1]
    if 'p' in tim.lower() and tim_list[0] != '12':
        tim_list[0] = int(tim_list[0]) + 12
    if 'a' in tim.lower() and tim_list[0] == '12':
        tim_list[0] = '00'
    tim_list.append('00')
    tim_str = ':'.join([str(i) for i in tim_list])
    return tim_str

def write_tuples(tuples : List[List[Any]], dt : datetime.date):
    yr_str = str(dt.year)
    mo_str = str(dt.month)
    write_to = f'./data/lineup_csvs/lineups_{yr_str}_{mo_str}.csv'
    
    new_file : bool = True
    if os.path.isfile(write_to):
        new_file = False
    
    if new_file:
        with open(write_to, 'w') as f:
            csvwriter = csv.writer(f)
            if new_file:
                csvwriter.writerow(OUT_COLS)
    
    with open(write_to, 'a') as f:
        csvwriter = csv.writer(f)
        csvwriter.writerows(tuples)
        

def process_games_at_link(link : str):
    games : ResultSet = get_games(link)
    dt : datetime = process_date(link)
    
    for game in games:
        tuples : List[List[Any ]]= get_csv_tuples_from_game(game, dt)
        write_tuples(tuples, dt)

def save_csv_to_s3(year : int, month : int, bucket : str = 'kniu-dfs', object_name = None):
    file_name = f'./data/lineup_csvs/lineups_{str(year)}_{str(month)}.csv'
    if object_name is None:
        object_name = os.path.basename(file_name)
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, 'lineups/' + object_name)
    except ClientError as e:
        print(e)
        return False
    return True


process_date(link)

if __name__ == '__main__':
    OUT_COLS = ['Away Team', 
        'Home Team',
        'Game Datetime',
        'Game Date',
        'Game Time',
        'Player Team',
        'Player Name',
        'Position',
        'Avg Price']

    # ========
    # 1. Read configs
    # ========
    config : Config = Config()
    pgc : PgConnection = PgConnection(config)
    dao : Dfs_dao = Dfs_dao(pgc)

    # ======
    # 2. Parse args
    # ======
    parser = argparse.ArgumentParser()
    parser.add_argument("year", help = "Year of data to be added ")
    parser.add_argument("--start_month", help = "Month of data to be added ", nargs = '?')
    parser.add_argument("--end_month", help = "End month of data to be added ", nargs = '?')
    args = parser.parse_args()
    YEAR : int = int(args.year)
    
    START_MONTH : int = 1
    if args.start_month:
        START_MONTH =int(args.start_month)
    
    END_MONTH : int = 12
    if args.end_month:
        END_MONTH =int(args.end_month)
    
    # ========
    # 3. Perform function
    # ========
    link_base : str = 'https://rotogrinders.com/lineups/nba?date=%s&site=fanduel'

    date_tuples : List[datetime.date] = dao.select_db_game_dates(YEAR)
    date_tuples = [dt for dt in date_tuples if (dt[0].month >= START_MONTH 
                                                and dt[0].month <= END_MONTH)]
    
    for date_tuple in date_tuples:
        dt : datetime = date_tuple[0]
        dt_str : str = dt.strftime('%Y-%m-%d')
        print(dt_str)
        new_link : str = link_base % dt_str
        process_games_at_link(new_link)
    
    for month_num in range(1, 13):
        try:
            save_csv_to_s3(YEAR, month_num)
        except:
            print(f"No files found for year: {YEAR} and month: {month_num}")