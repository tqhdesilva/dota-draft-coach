import pandas as pd

rad_cats = ['radiant_' + str(x) for x in xrange(0, 115)]
dire_cats = ['dire_' + str(x) for x in xrange(0, 115)]

def expand_players(df):
    df = df.copy()

def parse_players(players_list):
    hero_id = [player['hero_id'] for player in players_list]
    radiant = [player['player_slot'] < 128 for player in players_list]
    df = pd.DataFrame({'hero_id' : hero_id, 'radiant': radiant})
    radiant_heroes = df[df['radiant']]['hero_id']
    dire_heroes = df[~df['radiant']]['hero_id']
    return radiant_heroes, dire_heroes

def make_dummy(radiant_heroes, dire_heroes):
    # columns = dict()
    # for i in xrange(0, 115): # wtf is hero_id = 0?
        # columns[ 'radiant_' + str(i) ] = [0]
        # columns[ 'dire_' + str(i) ] = [0]
    # for hero in radiant_heroes:
        # columns['radiant_' + str(hero)][0] = 1
    # for hero in dire_heroes:
        # columns['dire_' + str(hero)][0] = 1
    # df = pd.DataFrame(columns)
    # return df
    rad_dummies = pd.get_dummies(radiant_heroes).reindex(columns=rad_cats,
                                           fill_value=0).sum()
    dire_dummies = pd.get_dummies(dire_heroes).reindex(columns=dire_cats,
                                           fill_value=0).sum()
    return pd.concat([rad_dummies, dire_dummies], axis=1)

def make_dummies(df):
    heroes = df['players'].map(parse_players)
    dummies_dfs = map(lambda x: make_dummy(*x), heroes)

    dummies_df = pd.concat(dummies_dfs, axis = 0)
    return dummies_df
