import dota2api
import os
import sqlalchemy
from db_helpers import connect, parse_date
import pandas as pd
from collections import defaultdict
import sys
import time

api = dota2api.Initialise(os.environ['D2_API_KEY'])

def build_matches(con):
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
    'match_seq_num' : pd.Series(dtype='int'),
    'start_time' : pd.Series(dtype='int'),
    'players': pd.Series(),
    'duration': pd.Series(dtype='int'),
    'radiant_win' : pd.Series(dtype='bool')})
    df = df.set_index('match_id')
    df.to_sql('matches', con, dtype={ 'players' : sqlalchemy.types.JSON}, if_exists='replace')

def request_matches_df(start_seq_num, n=100):
    '''
    Parameters
    ----------
    start_seq_num: int, number of entries to get
    n: number of matches to get

    Returns
    -------
    df: DataFrame, table of query results
    '''
    df_dict = defaultdict(list)
    # matches = api.get_match_history_by_seq_num(start_seq_num, game_mode=2, matches_requested=n)['matches']
    try:
        matches = api.get_match_history_by_seq_num(start_seq_num, game_mode=2, matches_requested=n)['matches']
    except ValueError:
        # print('bad call')
        return
    keys = ['match_id', 'match_seq_num', 'start_time', 'players', 'duration', 'radiant_win']
    for key in keys:
        for match in matches:
            df_dict[key].append(match[key])
    df = pd.DataFrame(df_dict)
    df = df.set_index('match_id')
    return df

def append_matches(con, df):
    '''
    Parameters
    ----------
    con: sqlalchemy engine object
    df: DataFrame, table to append to database

    Returns
    -------
    None
    '''
    df.to_sql('matches', con, dtype={'players' : sqlalchemy.types.JSON}, if_exists='append')

def append_matches_by_seq(con, start_seq, end_time, n=100, max_duration=None):
    '''
    con: sqlalchemy engine object
    start_seq: int, largest sequence number of matches to filter by
    end_seq: int, smallest sequence number of match to filter by
    end_time: int, start_time to bound our queries below by
    n: number of matches to get per request
    '''
    current_seq = start_seq
    current_time = end_time
    time_0 = time.time()
    elapsed_time = 0
    errors = 0
    while (max_duration == None or elapsed_time < max_duration):
        df = request_matches_df(current_seq, n)
        if not isinstance(df, pd.DataFrame):
            errors += 1
            # print('bad call')
            time.sleep(4.0)
            continue
        current_seq = df['match_seq_num'].max() + 1
        filtered_df = df[df['start_time'] <= end_time]
        append_matches(con, filtered_df)
        time.sleep(1.5)
        elapsed_time = time.time() - time_0
        if filtered_df.shape[0] < df.shape[0]:
            break

if __name__ == '__main__':
    try:
        db_name = sys.argv[2]
    except IndexError:
        db_name = 'dota2_draft'
    with open(os.path.expanduser('~/.pgpass')) as f:
        for line in f:
            host, port, db, user, password = [x.strip() for x in line.split(':')]
            if db == db_name:
                con, meta = connect(user=user, password=password, db=db, host=host, port=port)
                break

    action = sys.argv[1]

    if action == 'build':
        build_matches(con)

    elif action == 'append':
        duration = int(sys.argv[3])
        start_seq = 2778110720 if len(sys.argv) < 5 else int(sys.argv[4]) # just a random match_seq_num from our desired start date

        end_time = parse_date('2017-09-18')
        append_matches_by_seq(con, start_seq, end_time, n=100, max_duration=duration)
