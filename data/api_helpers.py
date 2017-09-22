import datetime
import pandas as pd
from collections import defaultdict
import time
import os
from data.db_helpers import append_db_match_history, append_db_match_details
import dota2api


api = dota2api.Initialise(os.environ['D2_API_KEY'])

def parse_date(date):
    return int(time.mktime(datetime.datetime.strptime(date, '%Y-%m-%d').timetuple()))

def api_call(args, out_q, con):
    method = args[0]
    args = args[1:]
    method(*args, out_q=out_q, con=con)

def parse_match_history(result):
    parsed_result = result['matches']
    df_dict = defaultdict(list)
    keys = ['match_id', 'match_seq_num', 'players', 'start_time']
    for row in parsed_result:
        for key in keys:
            df_dict[key].append(row[key])
    df = pd.DataFrame(df_dict)
    df = df.set_index('match_id')
    return df

def parse_match_details(result):
    keys = ['match_id', 'radiant_win', 'duration']
    parsed_result = {key : [result[key]] for key in keys}
    df = pd.DataFrame(parsed_result)
    df = df.set_index('match_id')
    return df

def get_match_history(match_seq_num, time0, duration, out_q, con):
    try:
        result = api.get_match_history(game_mode=2, start_at_match_seq_num=match_seq_num)
    except ValueError:
        args = (get_match_history, match_seq_num)
        out_q.put((1, args))
        return
    result_df = parse_match_history(result)

    append_db_match_history(result_df, con)
    for match_id in result_df.index:
        out_q.put((2, (get_match_details, match_id)))
    if time.time() - time0 < duration:
        args = (get_match_history, result_df['match_seq_num'].min(), time0, duration)
        out_q.put((3, args))


def get_match_details(match_id, out_q, con):
    print('getting match details')
    try:
        result = api.get_match_details(match_id)
    except ValueError:
        args = (get_match_details, match_id)
        out_q.put((1, args))
        in_q.task_done()
        return

    result_df = parse_match_details(result)
    append_db_match_details(result_df, con)
