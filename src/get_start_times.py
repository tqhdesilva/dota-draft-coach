from opendota_api import explorer_request, load_df, request_df
import sys
import os
import sqlalchemy
import pandas as pd
import time
import datetime
from db_helpers import connect, parse_date


def build_start_times(con):
    '''
    Parameters
    ----------
    con: sqlalchemy engine object

    Returns
    -------
    None

    Creates matches table. Overwrites if already exists.
    '''
    df = pd.DataFrame({'match_id': pd.Series(dtype='int'),
    'start_time': pd.Series(dtype='int')})
    df = df.set_index('match_id')
    df.to_sql('start_times', con, if_exists='replace')



def append_start_times( con, n, start_time, end_time, seq_num=0):
    '''
    Parameters
    ----------
    con: sqlalchemy engine object
    n: number of entries to get
    start_time: int, get matches occuring after this time
    end_time: int, get matches occuring before this time
    seq_num: int, grab next n matches with match_seq_num > seq_num

    Returns
    -------
    None
    '''
    # grab the next n matches with match_seq_num > seq_num
    query = '''
    SELECT match_id,
    start_time, match_seq_num
    FROM matches
    WHERE start_time BETWEEN {start_time} AND {end_time}
    AND match_seq_num > {seq_num}
    AND game_mode = 2
    AND picks_bans IS NOT NULL
    ORDER BY match_seq_num ASC
    LIMIT {n};
    '''.format(start_time=start_time, end_time=end_time, n=n, seq_num=seq_num)

    # build df
    df = request_df(query, 'match_id')

    max_seq_num = df['match_seq_num'].max()

    if df.shape[0] == 0:
        return


    # write dataframe to postgres
    df = df.drop('match_seq_num', axis=1)
    df.to_sql('start_times', con,  if_exists='append')
    return max_seq_num

if __name__ == '__main__':
    # we take in one argument, the name of the database to build
    # your pgpass needs to be set up properly to access the database we will be building
    start = 1351237677 #parse_date('2017-05-15')
    end = 1506284352 #parse_date('2017-09-18')
    try:
        max_time = float(sys.argv[2])
    except IndexError:
        max_time = None

    try:
        db_name = sys.argv[1]
    except IndexError:
        db_name = 'dota-draft'
    with open(os.path.expanduser('~/.pgpass')) as f:
        for line in f:
            host, port, db, user, password = [x.strip() for x in line.split(':')]
            if db == db_name:
                con, meta = connect(user=user, password=password, db=db, host=host, port=port)
                break

    build_start_times(con)

    elapsed_time = 0
    sleep_time = .34
    match_seq_start = 0
    while max_time == None or elapsed_time <= max_time:
        # request 10 matches
        match_seq_start = append_start_times(con, 200, start, end, match_seq_start)
        time.sleep(sleep_time)
        if match_seq_start == None:
            break
