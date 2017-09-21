from urllib import quote
import requests
import json
from collections import defaultdict
import pandas as pd

def explorer_request(request):
    '''
    Parameters: request(string)
    Returns: rows(list of outputs)
    '''
    explorer_url = 'https://api.opendota.com/api/explorer?sql='
    request_url = quote(request)
    r = requests.get(explorer_url + request_url)
    try:
        rows = json.loads(r.content)['rows']
    except ValueError:
        import pdb; pdb.set_trace()
    return rows

def load_df(rows):
    '''
    Parameters
    ----------
    rows: list, results of explorer_request

    Returns
    -------
    df: DataFrame, query results formatted as df
    '''
    if len(rows) == 0:
        return pd.DataFrame()
    df_dict = defaultdict(list)
    for key in rows[0]:
        for row in rows:
            df_dict[key].append(row[key])
    df = pd.DataFrame(data=df_dict)
    return df

def request_df(query, index=None):
    '''
    Parameters
    ----------
    query: str, valid sql query for opendota explorer endpoint
    index: str, default None, name of column to make index

    Returns
    -------
    df: DataFrame, query results in df form
    '''
    rows = explorer_request(query)
    df = load_df(rows)
    if df.empty:
        return pd.DataFrame()
    if index:
        df = df.set_index(index)
    return df

def get_sequential_matches():
    pass
