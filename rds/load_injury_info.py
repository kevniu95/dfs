import csv
import urllib
import datetime
from typing import List, Tuple, Any
import pandas as pd
import numpy as np
from tabula.io import read_pdf, convert_into

from config import Config
from pgConnect import PgConnection
from dfs_dao import Dfs_dao


def process_times_of_day(game_time_1 : datetime.time, game_time_2 : datetime.time) -> List[str]:
    hr1 = game_time_1.hour - 1
    hr2 = game_time_2.hour - 1
    
    retHours = []
    ampm = 'AM'
    while hr1 < hr2 + 2:
        printHr = hr1
        if hr1 >= 12:
            ampm = 'PM'
        if hr1 > 12:
            printHr -= 12
        retHours.append((str(printHr).zfill(2) + ampm, hr1))
        hr1 += 1
    return retHours

def extract_date_from_link(link : str) -> datetime.date:
    date_portion : str = link[link.rfind('t_')+2 : link.rfind('_')]
    dp_list : List[str] = [int(i) for i in date_portion.split('-')]
    dt : datetime.date = datetime.date(*tuple(dp_list))
    return dt

def get_injury_report_links(date_tuples : List[Tuple[Any,... ]]) -> List[Tuple[str, datetime.time]]:
    collection_hours = []
    time_stamps = []
    for date_tuple in date_tuples:
        yr, mth, day = (str(date_tuple[0].year), 
                        str(date_tuple[0].month).zfill(2), 
                        str(date_tuple[0].day).zfill(2))
        
        day_base = '-'.join([yr, mth, day])
        hours = process_times_of_day(date_tuple[1], date_tuple[2])
        for hour in hours:
            collection_hours.append(injury_base + '_' + day_base + '_' + hour[0] + '.pdf')
            time_stamps.append(datetime.time(hour[1],30))
    return list(zip(collection_hours, time_stamps))

def collect_injury_report_csv(link : str, report_time : datetime.time):
    dt : datetime.date = extract_date_from_link(link)

    response = urllib.request.urlopen(link)
    a = response.read()
    convert_into(link, 'test.csv', pages = 'all')

    with open('test.csv') as f:
        csv_rows = []
        r = csv.reader(f)
        for num, row in enumerate(r):
            row = [''] * 7 + row
            while row[-1] == '':
                row.pop(-1)
            row = row[-7:]
            if row[-1] not in ('NOT YET SUBMITTED', 'Reason') or num == 0:
                csv_rows.append(row)

    df = pd.DataFrame(csv_rows[1:], columns = csv_rows[0])
    df = df.replace('', np.nan).fillna(method = 'ffill')
    df['report_date'] = dt
    df['report_time'] = report_time
    df['Report Datetime'] = datetime.datetime.combine(dt, report_time)
    tmp = df['Game Time'].str.replace(' (ET)', 'pm', regex = False)
    df['Game Datetime'] = pd.to_datetime(df['Game Date'] + ' ' + tmp, format = '%m/%d/%Y %I:%M%p')
    cols = ['Report Datetime', 
            'Game Datetime',
            'Game Date',
            'Game Time',
            'Matchup',
            'Team',
            'Player Name',
            'Current Status',
            'Reason']
    return df[cols]

if __name__ == '__main__':
    config : Config = Config()
    pgc : PgConnection = PgConnection(config)
    dao : Dfs_dao = Dfs_dao(pgc)

    injury_base = 'https://ak-static.cms.nba.com/referee/injury/Injury-Report'

    date_tuples : List[Tuple[str, datetime.time]]= dao.select_game_date_times()

    injury_links = get_injury_report_links(date_tuples)
    
    test_link : Tuple[str, datetime.time] = injury_links[0]

    a = collect_injury_report_csv(test_link[0], test_link[1])

    print(a.head())