import os
import sys
import pandas as pd
import numpy as np
from build_db import connect
from db_helpers import parse_date
from scipy.sparse import csc_matrix, hstack

def polynomial_features(X):
    X_sparse = csc_matrix(X)
    sparse_product = []
    for i in xrange(X.shape[1] -1 ):
        for j in xrange(i, X.shape[1]):
            sparse_product.append(X_sparse[:, i].multiply(X_sparse[:, j]))
    X_sparse_poly = hstack(sparse_product)
    return X_sparse_poly

def parse_pb(pb_list):
    # return t1_picks, t2_picks, t1_bans, t2_bans
    pb_df = pd.DataFrame(data=map(lambda x: x.values(), pb_list), columns=pb_list[0].keys())
    picks = pb_df[pb_df['is_pick']]
    bans = pb_df[~pb_df['is_pick']]
    team1 = picks['team'].iloc[0]
    team2 = picks['team'].iloc[1]
    t1_picks, t2_picks = tuple(picks[picks['team']==team1]['hero_id']),\
                         tuple(picks[picks['team']==team2]['hero_id'])

    t1_bans = []
    t2_bans = []

    ban_schema = [team1, team2, team1, team2, team2, team1, team2, team1, team1, team2]
    ban_counter = 0
    for banning_team in ban_schema:
        try:
            next_ban = bans.iloc[ban_counter]
            if next_ban['team'] == banning_team:
                banned_hero = next_ban['hero_id']
                ban_counter += 1
            else:
                banned_hero = None
        except IndexError:
            banned_hero = None
        if banning_team == team1:
            t1_bans.append(banned_hero)
        else:
            t2_bans.append(banned_hero)
    t1_bans = tuple(t1_bans)
    t2_bans = tuple(t2_bans)
    return {'t1_picks': t1_picks, 't2_picks': t2_picks, 't1_bans': t1_bans,
            't2_bans': t2_bans, 'team1': team1, 'team2': team2}

def parse_players(player_list):
    radiant_heroes = [player['hero_id'] for player in player_list if player['player_slot'] < 128]
    dire_heroes = [player['hero_id'] for player in player_list if player['player_slot'] >= 128]
    return radiant_heroes, dire_heroes

def preprocess_players(df):
    df = df.copy()
    parsed_series = df['players'].map(parse_players)
    radiant_picks = parsed_series.map(lambda x: x[0])
    dire_picks = parsed_series.map(lambda x: x[1])
    picks_df = pd.DataFrame(data={'radiant_picks': radiant_picks,
                                  'dire_picks': dire_picks})
    radiant_dummies = pd.get_dummies(picks_df['radiant_picks'].apply(pd.Series).stack().astype('category', categories=range(1,115)),
                                     prefix='radiant', prefix_sep='_').sum(level=0)
                                     # .astype('category', categories=range(1, 115))
    dire_dummies = pd.get_dummies(picks_df['dire_picks'].apply(pd.Series).stack().astype('category', categories=range(1,115)),
                                  prefix='dire', prefix_sep='_').sum(level=0)
    df = pd.concat([df[['match_id', 'radiant_win']], radiant_dummies, dire_dummies],
                   axis=1)
    return df

def preprocess(df):
    df = df.copy()
    parsed_series = df['picks_bans'].map(parse_pb)
    t1_picks = parsed_series.map(lambda x: x['t1_picks'])
    t2_picks = parsed_series.map(lambda x: x['t2_picks'])
    team1 = parsed_series.map(lambda x: x['team1'])
    picks_df = pd.DataFrame(data={'t1_picks': t1_picks, 't2_picks':t2_picks, 'team1': team1})
    t1_dummies = pd.get_dummies(picks_df['t1_picks'].apply(pd.Series).stack().astype('category', categories=range(1,115)), prefix='t1', prefix_sep='_').sum(level=0)
    t2_dummies = pd.get_dummies(picks_df['t2_picks'].apply(pd.Series).stack().astype('category', categories=range(1,115)), prefix='t2', prefix_sep='_').sum(level=0)
    df['team1_win'] = (picks_df['team1'] == 1) ^ (df['radiant_win'])
    df = pd.concat([df[['match_id', 'team1_win']], t1_dummies, t2_dummies], axis=1)
    df['team1'] = team1
    return df

