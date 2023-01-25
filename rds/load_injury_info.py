import os
import csv
import datetime
from typing import List, Tuple, Any, Dict
import pandas as pd
import numpy as np
import argparse
import boto3
from botocore.exceptions import ClientError
from tabula.io import read_pdf, convert_into

from config import Config
from pgConnect import PgConnection
from dfs_dao import Dfs_dao

pd.set_option('display.max_columns', None)


def process_times_of_day(game_time_1 : datetime.time, game_time_2 : datetime.time) -> List[str]:
    hr1 = game_time_1.hour - 2
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

def get_injury_report_links(date_tuples : List[Tuple[datetime.date, 
                                                        datetime.time, 
                                                        datetime.time ]]) \
                                                        -> Dict[datetime.datetime, List[str]]:
    return_dict : Dict[datetime.datetime, List[str]] = {}
    
    for date_tuple in date_tuples:
        yr, mth, day = (str(date_tuple[0].year), 
                        str(date_tuple[0].month).zfill(2), 
                        str(date_tuple[0].day).zfill(2))
        
        hours : List[str] = process_times_of_day(date_tuple[1], date_tuple[2])
        for hour in hours:
            link : str = injury_base + '_' + '-'.join([yr, mth, day]) + \
                             '_' + hour[0] + '.pdf'
            time_stamp = datetime.datetime.combine(date_tuple[0],
                                                    datetime.time(hour[1],30))
            return_dict[time_stamp] = link
    return return_dict

def collect_injury_report_csv(link : str, report_datetime : datetime.datetime) -> pd.DataFrame:
    try:
        convert_into(link, f'./data/csv_templates/read_{YEAR}.csv', pages = 'all')
    except Exception as e:
        print(f"Didn't convert {link} into csv!")
        print(e)
        return
    
    with open(f'./data/csv_templates/read_{YEAR}.csv') as f:
        csv_rows = []
        r = csv.reader(f)
        for num, row in enumerate(r):
            row = [''] * 9 + row
            while row[-1] == '':
                row.pop(-1)
            row = row[-9:]
            if (row[-1] not in ('Reason', 'Previous Status', 'Previous Reason') and 'NOT YET SUBMITTED' not in row) or num in (0, 1):
                csv_rows.append(row)
            
    csv_rows[0] = [i.strip() for i in csv_rows[0]]
    
    # A. Create and fill df
    df = pd.DataFrame(csv_rows[1:], columns = csv_rows[0])
    df = df.replace('', np.nan).fillna(method = 'ffill')
    df['Report Datetime'] = report_datetime
    df['Report Link'] = link
    
    # A. Keep if report and game dates match
    df = df[pd.to_datetime(df['Game Date']).dt.date == df['Report Datetime'].dt.date] 
    
    # B. Create Game datetime field
    tmp = df['Game Time'].str.replace('([ ]*\(ET\)[ ]*)', 'pm', regex = True)
    df['Game Datetime'] = pd.to_datetime(df['Game Date'].fillna('01/01/1900') + ' ' + tmp.fillna('0:0a'), 
                                            format = '%m/%d/%Y %I:%M%p')
    return df[OUT_COLS]


def process_month_year(month : int, year : int, injury_links : Dict[datetime.datetime, List[str]]) -> str:
    file_name = f'./data/csv_templates/write_{year}_{month}.csv'
    with open(file_name, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(OUT_COLS)

    
    link_sub = {k : v for (k, v) in injury_links.items() if k.month == month and k.year == year}
    for time_stamp, link in link_sub.items():
        print(link)
        df = collect_injury_report_csv(link, time_stamp)
        if df is not None and len(df) > 0:
            df.to_csv(file_name, 
                        index = False, 
                        mode = 'a', 
                        header  = False)
        print()
    return file_name
    
def save_csv_to_s3(file_name : str, bucket : str, object_name = None):
        if object_name is None:
            object_name = os.path.basename(file_name)
        # Upload the file
        s3_client = boto3.client('s3')
        try:
            response = s3_client.upload_file(file_name, bucket, 'player-injuries/' + object_name)
        except ClientError as e:
            print(e)
            return False
        return True

def get_month_years(injury_links : Dict[datetime.datetime, List[str]]) -> List[Tuple[int, int]]:
    month_years = zip([i.month for i in list(injury_links.keys())], [i.year for i in list(injury_links.keys())])
    month_years = list(set(month_years))
    month_years.sort(key = lambda x : (x[1], x[0]))
    return month_years    


if __name__ == '__main__':
    OUT_COLS = ['Report Datetime', 
        'Game Datetime',
        'Game Date',
        'Game Time',
        'Matchup',
        'Team',
        'Player Name',
        'Current Status',
        'Reason',
        'Report Link']

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
    args = parser.parse_args()
    YEAR : int = int(args.year)
    
    START_MONTH : int = 1
    if args.start_month:
        START_MONTH =int(args.start_month)
    
    # ========
    # 3. Perform function
    # ========
    injury_base = 'https://ak-static.cms.nba.com/referee/injury/Injury-Report'

    date_tuples : List[Tuple[datetime.date, datetime.time, datetime.time]] = dao.select_db_game_date_times()
    
    injury_links = get_injury_report_links(date_tuples)
    
    month_years : List[Tuple[int, int]] = get_month_years(injury_links)
    
    for month_year in month_years:
        month = month_year[0]
        year = month_year[1]
        print(month, year)
        if month_year[1] == YEAR and month_year[0] >= START_MONTH:
            csv_file : str = process_month_year(month, year, injury_links)
            save_csv_to_s3(csv_file, 'kniu-dfs')