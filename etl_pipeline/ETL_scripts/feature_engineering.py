import pandas as pd
from .utils import resolution_to_minutes, utc_to_local

def get_time_of_day(hour):
    """Categorize hour into time of day buckets."""
    if 5 <= hour < 12:
        return 'morning'
    elif 12 <= hour < 17:
        return 'afternoon'
    elif 17 <= hour < 21:
        return 'evening'
    else:
        return 'night'

def get_season(month):
    """Return season for a given month."""
    if month in [12, 1, 2]:
        return 'winter'
    elif month in [3, 4, 5]:
        return 'spring'
    elif month in [6, 7, 8]:
        return 'summer'
    else:
        return 'autumn'

def feature_engineering_and_aggregation(df: pd.DataFrame, local_tz: str = "Europe/Vilnius") -> pd.DataFrame:
    df['resolution_in_min'] = df['resolution'].apply(resolution_to_minutes)
    df.drop(columns=['resolution'], inplace=True)
    df_exploded = df.explode('energy_consumption').reset_index(drop=True)
    df_exploded['energy_consumption'] = df_exploded['energy_consumption'].astype(float)
    df_exploded['step'] = df_exploded.groupby(['client_id','ext_dev_ref','date']).cumcount()
    df_exploded['minute_offset'] = df_exploded['step'] * df_exploded['resolution_in_min']
    df_exploded['timestamp'] = pd.to_datetime(df_exploded['date'], utc=True) + \
                               pd.to_timedelta(df_exploded['minute_offset'], unit='m')
    if local_tz:
        df_exploded = utc_to_local(df_exploded, 'timestamp', local_tz)
        df_exploded['local_date'] = df_exploded['timestamp'].dt.date
        grouping_key = ['client_id', 'ext_dev_ref', 'local_date']
    else:
        grouping_key = ['client_id', 'ext_dev_ref', 'date']
    df_exploded['hour'] = df_exploded['timestamp'].dt.hour
    df_exploded['month'] = df_exploded['timestamp'].dt.month
    df_exploded['day_of_week'] = df_exploded['timestamp'].dt.dayofweek
    df_exploded['is_weekend'] = df_exploded['day_of_week'].isin([5, 6]).astype(int)
    df_exploded['time_of_day'] = df_exploded['hour'].apply(get_time_of_day)
    df_exploded['season'] = df_exploded['month'].apply(get_season)
    df_exploded['peak_flag'] = df_exploded['time_of_day'].apply(
        lambda x: 1 if x in ['morning', 'evening'] else 0
    )
    daily_agg = (
        df_exploded.groupby(grouping_key)['energy_consumption']
        .agg(['sum', 'mean', 'max', 'min'])
        .reset_index()
        .rename(columns={'sum': 'daily_sum', 'mean': 'daily_mean', 'max': 'daily_max', 'min': 'daily_min'})
    )
    tod_agg = (
        df_exploded.groupby(grouping_key + ['time_of_day'])['energy_consumption']
        .agg(['sum', 'mean', 'max', 'min'])
        .reset_index()
        .rename(columns={'sum': 'tod_sum', 'mean': 'tod_mean', 'max': 'tod_max', 'min': 'tod_min'})
    )
    season_agg = (
        df_exploded.groupby(grouping_key + ['season'])['energy_consumption']
        .agg(['sum', 'mean', 'max', 'min'])
        .reset_index()
        .rename(columns={'sum': 'season_sum', 'mean': 'season_mean', 'max': 'season_max', 'min': 'season_min'})
    )
    output_df = pd.merge(df_exploded, daily_agg, on=grouping_key, how='left')
    output_df = pd.merge(output_df, tod_agg, on=grouping_key + ['time_of_day'], how='left')
    output_df = pd.merge(output_df, season_agg, on=grouping_key + ['season'], how='left')
    return output_df