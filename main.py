import os
import polars as pl
from datetime import datetime
from itertools import combinations
from utils.json_utils import read_orjson

import warnings
# Suppress all warnings
warnings.filterwarnings("ignore")

BASE_PATH = 'E:/panag/Desktop/Github_repos/telegram_arbitrage_bot/' # Add your own path

pregame_stoiximan = read_orjson(os.path.join(BASE_PATH, 'dataset', 'manipulate_data', 'pregame_stoiximan.json'))
pregame_novibet = read_orjson(os.path.join(BASE_PATH, 'dataset', 'manipulate_data', 'pregame_novibet.json'))
pregame_pamestoixima = read_orjson(os.path.join(BASE_PATH, 'dataset', 'manipulate_data', 'pregame_pamestoixima.json'))
pregame_fonbet = read_orjson(os.path.join(BASE_PATH, 'dataset', 'manipulate_data', 'pregame_fonbet.json'))

# Define schemas for datasets
schema_stoiximan = {
    'stoiximan_sport': pl.Utf8,
    'start_date_time': pl.Utf8,
    'url': pl.Utf8,
    'home_team': pl.Utf8,
    'away_team': pl.Utf8,
    'market_suspended': pl.Utf8,
    'market': pl.Utf8,
    'market_play': pl.Utf8,
    'stoiximan_odds': pl.Float64
}

schema_novibet = {
    'novibet_sport': pl.Utf8,
    'start_date_time': pl.Utf8,
    'url': pl.Utf8,
    'home_team': pl.Utf8,
    'away_team': pl.Utf8,
    'market_available': pl.Boolean,
    'market': pl.Utf8,
    'market_play': pl.Utf8,
    'novibet_odds': pl.Float64
}

schema_pamestoixma = {
    'url_id': pl.Utf8,
    'url_game': pl.Utf8,
    'url_sport': pl.Utf8,
    'url_competition': pl.Utf8,
    'start_date_time': pl.Utf8,
    'home_team': pl.Utf8,
    'away_team': pl.Utf8,
    'game_status': pl.Utf8,
    'market_status': pl.Utf8,
    'market': pl.Utf8,
    'market_play': pl.Utf8,
    'pamestoixima_odds': pl.Float64
}

schema_fonbet = {
                    "start_date_time" : pl.Utf8,
                    "url_game_id" : pl.Utf8,
                    "url_sport_id" : pl.Utf8,
                    "home_team": pl.Utf8,
                    "away_team": pl.Utf8,
                    "game_status": pl.Utf8,
                    "market": pl.Utf8,
                    "market_play": pl.Utf8,
                    "fonbet_odds": pl.Float64
                    }


# 1. Convert deque to Polars DataFrame for easier manipulation
df_nv = pl.from_dicts(pregame_novibet, schema=schema_novibet)
df_st = pl.from_dicts(pregame_stoiximan, schema=schema_stoiximan)
df_pm = pl.from_dicts(pregame_pamestoixima, schema=schema_pamestoixma)
df_fb = pl.from_dicts(pregame_fonbet, schema=schema_fonbet)

# 2. Keep only Avalaible markets // Fonbet Pregame
df_st = df_st.filter((pl.col('market_suspended') != 'true'))
df_nv = df_nv.filter((pl.col('market_available') == 'true'))
df_pm = df_pm.filter((pl.col('game_status') == 'ACTIVE') & (pl.col('market_status') == 'ACTIVE'))
df_fb = df_fb.filter((pl.col('game_status') == 'line'))

# 3. Exclude the market with more than 2 outcomes
markets = ['MATCH_ODDS','DOUBLE_CHANCE','HALF_TIME']
df_st = df_st.filter(~pl.col('market').is_in(markets))
df_nv = df_nv.filter(~pl.col('market').is_in(markets))
df_pm = df_pm.filter(~pl.col('market').is_in(markets))
df_fb = df_fb.filter(~pl.col('market').is_in(markets))

# 4. Create date_only column for the join condition // Not in LIVE
df_nv = df_nv.with_columns(pl.col('start_date_time').str.slice(0, 5).alias('date_only'))
df_st = df_st.with_columns(pl.col('start_date_time').str.slice(0, 5).alias('date_only'))
df_pm = df_pm.with_columns(pl.col('start_date_time').str.slice(0, 5).alias('date_only'))
df_fb = df_fb.with_columns([
        (pl.from_epoch(pl.col('start_date_time').cast(pl.Int64), time_unit='s')
        .dt.offset_by('2h')
        .dt.strftime('%d-%m')
        .alias('date_only'))
    ])

def find_arbitrage_between_pair(df1, df2, name1, name2):
    """
    Find arbitrage opportunities between two bookmakers.
    """
    # Join the dataframes
    merged = df1.join(
        df2,
        on=['home_team', 'away_team', 'date_only', 'market', 'market_play'],
        how='inner'
    )
    
    if merged.height == 0:
        return None
    
    # Calculate max odds between the two bookmakers
    odds1 = f"{name1}_odds"
    odds2 = f"{name2}_odds"
    
    final_df = (
        merged
        .with_columns([
            pl.when(pl.col(odds1) > pl.col(odds2))
                .then(pl.col(odds1))
                .otherwise(pl.col(odds2))
                .alias('odds_max'),
            pl.when(pl.col(odds1) > pl.col(odds2))
                .then(pl.lit(name1))
                .otherwise(pl.lit(name2))
                .alias('bookmaker')
        ])
        .with_columns([
            (1 / pl.col('odds_max')).alias('inverse_odds')
        ])
        .group_by(['home_team', 'away_team', 'market'])
        .agg([
            pl.sum('inverse_odds').alias('total_inverse_odds'),
            pl.col('market_play').alias('plays'),
            pl.col('odds_max').alias('odds'),
            pl.col('bookmaker').alias('bookmakers')
        ])
        .filter(pl.col('total_inverse_odds') < 1)
        .with_columns([
            ((1 - pl.col('total_inverse_odds')) * 100).alias('arbitrage_profit_percent')
        ])
        .sort('arbitrage_profit_percent', descending=True)
    )
    
    return final_df if final_df.height > 0 else None

def find_all_arbitrage_opportunities(bookmakers_data):
    """
    Find arbitrage opportunities across all pairs of bookmakers.
    """
    opportunities = []
    
    # Generate all possible pairs of bookmakers
    for (name1, df1), (name2, df2) in combinations(bookmakers_data.items(), 2):
        
        result = find_arbitrage_between_pair(df1, df2, name1, name2)
        if result is not None:
            opportunities.append({
                'bookmaker_pair': f"{name1} vs {name2}",
                'opportunities': result
            })
    
    return opportunities


bookmakers_data = {
    'stoiximan': df_st,      # Your stoiximan DataFrame
    'novibet': df_nv,        # Your novibet DataFrame
    'pamestoixima': df_pm,   # Your pamestoixima DataFrame 
    'fonbet': df_fb          # Your fonbet DataFrame
}
# Use the function
results = find_all_arbitrage_opportunities(bookmakers_data)
print(f'########### Calculations: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
# Print results
for result in results:
    print(f"\nArbitrage opportunities for {result['bookmaker_pair']}:")
    print(result['opportunities'])