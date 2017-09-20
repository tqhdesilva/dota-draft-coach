from opendota_api import explorer_request, load_df, request_df
import sys
import os
import sqlalchemy
import pandas as pd
import time
import datetime



def parse_date(date):
    return int(time.mktime(datetime.strptime(date, '%Y-%m-%d').timetuple()))

def connect(user, password, db, host='localhost', port=5432):
    '''Returns a connection and a metadata object'''
    # We connect with the help of the PostgreSQL URL
    # postgresql://federer:grandestslam@localhost:5432/tennis
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(user, password, host, port, db)

    # The return value of create_engine() is our connection object
    con = sqlalchemy.create_engine(url, client_encoding='utf8')

    # We then bind the connection to MetaData()
    meta = sqlalchemy.MetaData(bind=con, reflect=True)

    return con, meta

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
    'picks_bans': pd.Series(),
    'duration': pd.Series(dtype='int'),
    'radiant_win' : pd.Series(dtype='bool')})
    df = df.set_index('match_id')
    df.to_sql('matches', con, dtype={ 'picks_bans' : sqlalchemy.types.JSON}, if_exists='replace')

def append_matches( con, n, start_time, end_time, seq_num=0):
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
    size: int, size of query result
    max_seq_num: int, largest match_seq_num returned in query
    '''
    # grab the next n matches with match_seq_num > seq_num
    query = '''
    SELECT match_id, duration,
    picks_bans, radiant_win,
    match_seq_num
    FROM matches
    WHERE start_time BETWEEN {start_time} AND {end_time}
    AND match_seq_num > {seq_num}
    AND game_mode = 2
    ORDER BY match_seq_num ASC
    LIMIT {n};
    '''.format(start_time=start_time, end_time=end_time, n=n, seq_num=seq_num)

    # build df
    df = request_df(query, 'match_id')
    if df.shape == 0:
        return

    # calculate return values
    max_seq_num = df['match_seq_num'].max()

    # write dataframe to postgres
    df = df.drop('match_seq_num', axis=1)
    df.to_sql('matches', con, dtype={'picks_bans' : sqlalchemy.types.JSON}, if_exists='append')

    return max_seq_num

def build_player_matches(con):
    '''
    Parameters
    ----------
    con: sqlalchemy engine object

    Returns
    -------
    None
    '''
    df = pd.DataFrame({'account_id': pd.Series(dtype='int'),
    'match_id': pd.Series(dtype='int'),
    'hero_id' : pd.Series(dtype='int')})
    df = df.set_index('account_id')
    df.to_sql('player_matches', con, if_exists='replace')

def append_player_matches(con, n, start_time, end_time, seq_num=0):
    '''
    Parameters
    ----------
    con: sqlalchemy engine object
    n: number of matches to query for (note that we will query 10 players for each match)
    start_time: int, get player_matches occuring after this time
    end_time: int, get player_matches occuring before this time
    seq_num: int, grab next n matches with match_seq_num > seq_num

    Returns
    -------
    size: int, size of query result
    max_seq_num: int, largest match_seq_num returned in query
    '''

    query = '''
    SELECT p.account_id as account_id, p.match_id as match_id,
    p.hero_id as hero_id, m.match_seq_num as match_seq_num
    FROM player_matches as p
    JOIN matches as m
    ON p.match_id = m.match_id
    WHERE start_time BETWEEN {start_time} AND {end_time}
    AND match_seq_num > {seq_num}
    AND m.game_mode = 2
    ORDER BY match_seq_num ASC
    LIMIT {n}
    '''.format(n=10*n, seq_num=seq_num, start_time=start_time, end_time=end_time)

    # load df
    df = request_df(query, 'account_id')
    if df.shape[0] == 0:
        return

    # calculate return values
    max_seq_num = df['match_seq_num'].max()

    # write dataframe to postgres
    df = df.drop('match_seq_num', axis=1)
    df.to_sql('player_matches', con, if_exists='append')

    return  max_seq_num

def build_heroes(con):
    '''
    Parameters
    ----------
    con: sqlalchemy engine object,

    Returns
    -------
    None
    '''
    query = '''
    SELECT * FROM heroes;
    '''

    df = request_df(query, 'id')
    df.to_sql('heroes', con, if_exists='replace')

def build_hero_ranking(con):
    '''
    Parameters
    ----------
    con: sqlalchemy engine object

    Returns
    -------
    None

    Creates hero_ranking table. Overwrites if already exists.
    '''
    df = pd.DataFrame({'account_id': pd.Series(dtype='int'),
    'hero_id': pd.Series(dtype='int'),
    'score' : pd.Series(dtype='float')})
    df = df.set_index('account_id')
    df.to_sql('hero_ranking', con, if_exists='replace')

def append_hero_ranking(con, n, start_time, end_time, seq_num=1):
    '''
    Parameters
    ----------
    con: sqlalchemy engine object
    n: int, number of players to query for (multiple queries per player)
    start_time: int, get player_matches occuring after this time
    end_time: int, get player_matches occuring before this time
    seq_num: int, grab next n matches with match_seq_num > seq_num

    Returns
    -------
    size: int, size of query result
    max_seq_num: int, largest match_seq_num returned in query
    '''
    query = '''
    SELECT hr.account_id as account_id,
    hr.hero_id as hero_id,
    hr.score as score,
    ms.min_match_seq_num as min_match_seq_num
    FROM (
        SELECT pm.account_id as account_id,
        MIN(m.match_seq_num) as min_match_seq_num
        FROM player_matches as pm
        JOIN matches as m
        ON m.match_id = pm.match_id
        WHERE m.start_time BETWEEN {start_time} AND {end_time}
        AND m.match_seq_num > {seq_num}
        GROUP BY account_id
        ORDER BY min_match_seq_num ASC
        LIMIT {n}
        ) as ms
    JOIN hero_ranking as hr
    ON ms.account_id = hr.account_id
    '''.format(n=n, start_time=start_time, end_time=end_time, seq_num=seq_num)

    df = request_df(query, 'account_id')
    if df.shape[0] == 0:
        return

    # calculate return values
    max_seq_num = df['min_match_seq_num'].max()
    size = df.shape[0]

    df = df.drop('min_match_seq_num', axis=1)
    df.to_sql('hero_ranking', con, if_exists='append')

    return max_seq_num

def build_mmr_estimates(con):
    '''
    Parameters
    ----------
    con: sqlalchemy engine object

    Returns
    -------
    None

    Creates mmr_estimates table. Overwrites if already exists.
    '''
    df = pd.DataFrame({'account_id': pd.Series(dtype='int'),
    'estimate': pd.Series(dtype='int')})
    df = df.set_index('account_id')
    df.to_sql('mmr_estimates', con, if_exists='replace')

def append_mmr_estimates(con, n, start_time, end_time, seq_num=1):
    '''
    Parameters
    ----------
    con: sqlalchemy engine object
    n: int, number of players to query for (multiple queries per player)
    start_time: int, get player_matches occuring after this time
    end_time: int, get player_matches occuring before this time
    seq_num: int, grab next n matches with match_seq_num > seq_num

    Returns
    -------
    size: int, size of query result
    max_seq_num: int, largest match_seq_num returned in query
    '''
    query = '''
    SELECT me.account_id as account_id,
    me.estimate as estimate,
    ms.min_match_seq_num as min_match_seq_num
    FROM
        (
            SELECT pm.account_id as account_id,
            MIN(m.match_seq_num) as min_match_seq_num
            FROM player_matches as pm
            JOIN matches as m
            ON m.match_id = pm.match_id
            WHERE m.start_time BETWEEN {start_time} AND {end_time}
            AND m.match_seq_num > {seq_num}
            GROUP BY account_id
            ORDER BY min_match_seq_num ASC
            LIMIT {n}
        ) as ms
        JOIN mmr_estimates as me
        ON me.account_id = ms.account_id
    '''.format(n=n, start_time=start_time, end_time=end_time, seq_num=seq_num)

    df = request_df(query, 'account_id')
    if df.shape[0] == 0:
        return

    # calculate return values
    max_seq_num = df['min_match_seq_num'].max()

    df = df.drop('min_match_seq_num', axis=1)
    df.to_sql('mmr_estimates', con, if_exists='append')

    return  max_seq_num

def create_db(con):
    '''
    Parameters
    ----------
    con: sqlalchemy engine object

    Returns
    -------
    None

    Calls the build function for all our tables
    '''

    build_matches(con)
    build_player_matches(con)
    build_heroes(con)
    build_hero_ranking(con)
    build_mmr_estimates(con)


if __name__ == '__main__':
    # we take in one argument, the name of the database to build
    # your pgpass needs to be set up properly to access the database we will be building

    start = parse_date('2017-05-15')
    end = parse_date('2017-09-18')
    try:
        max_time = sys.argv[2]
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

    build_db(con)

    # 3 requests per second
    # per match, at most 10 new players
    # per player, at most 113 heroes
    # per player, exactly one mmr estimate
    match_seq = 0
    pm_seq = 0
    hr_seq = 0
    mmr_seq = 0
    time_0 = datetime.datetime.now()
    elapsed_time = 0
    sleep_time = .34
    while match_seq != None and pm_seq != None and hr_seq != None and mmr_seq != None and (max_time == None or elapsed_time <= max_time):
        # request 10 matches
        time.sleep(sleep_time)

        # request 100 players

        time.sleep(sleep_time)

        # request 100 player's hero ratings

        time.sleep(sleep_time)

        # request 100 mmr estimates

        time.sleep(sleep_time)

        elapsed_time = time.time()
