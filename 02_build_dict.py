import re
import calendar
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.neighbors import NearestNeighbors

# STEP 0: Define functions
def split_station_info(df): # Define function to help split the station name
    # Define a regex pattern to split the column
    pattern = r'^(.*?),\s(.*?)\s(\w\w)$'
    
    # Extract values into new columns using the regex pattern
    df_extract = df['station_name'].str.extract(pattern)
    
    return df.assign(
        station_name=df_extract[0],
        state_province=df_extract[1],
        country=df_extract[2]
    )

def freeze_days(sub_df): # Function to calculate first and last day of year where min_temp > 32.0
    above_freezing = sub_df[sub_df['min_temp'] > 32.0]
    
    last_freeze_month = above_freezing['month'].iloc[0] if not above_freezing.empty else None
    last_freeze_day = above_freezing['day'].iloc[0] if not above_freezing.empty else None
    last_freeze_temp = above_freezing['min_temp'].iloc[0] if not above_freezing.empty else None
    
    first_freeze_month = above_freezing['month'].iloc[-1] if not above_freezing.empty else None
    first_freeze_day = above_freezing['day'].iloc[-1] if not above_freezing.empty else None
    first_freeze_temp = sub_df[(sub_df['month'] == first_freeze_month) & (sub_df['day'] == first_freeze_day)]['min_temp'].iloc[0] if not above_freezing.empty and first_freeze_month and first_freeze_day else None
    
    last_freeze_str = f"{calendar.month_name[last_freeze_month]} {last_freeze_day}" if last_freeze_month and last_freeze_day else None
    first_freeze_str = f"{calendar.month_name[first_freeze_month]} {first_freeze_day}" if first_freeze_month and first_freeze_day else None
    
    # Check if last_freeze is "January 1" and first_freeze is "December 31"
    if last_freeze_str == "January 1" and first_freeze_str == "December 31":
        last_freeze_str = "No frost / very infrequent"
        first_freeze_str = "No frost / very infrequent"
        last_freeze_temp = "N/A"
        first_freeze_temp = "N/A"
    
    return pd.Series({'last_freeze': last_freeze_str, 'last_freeze_min_temp': last_freeze_temp, 'first_freeze': first_freeze_str, 'first_freeze_min_temp': first_freeze_temp})

def format_date(date_str): # Function to display dates nicely 
    # Check if the string matches MM/DD format using regex
    if re.match(r"^\d{2}/\d{2}$", date_str):
        return datetime.strptime(date_str, "%m/%d").strftime("%B %d")
    else:
        return date_str

def date_difference(start, end): # Function calculate growing_days 
    try:
        # Convert to datetime, assuming the current year
        start_date = datetime.strptime(start + ' 2023', "%B %d %Y")
        end_date = datetime.strptime(end + ' 2023', "%B %d %Y")
        
        # Calculate difference
        diff = (end_date - start_date).days
        
        # If difference is negative, assume the end_date is from the next year
        if diff < 0:
            end_date = datetime.strptime(end + ' 2024', "%B %d %Y")
            diff = (end_date - start_date).days
        
        return diff
    
    except ValueError:  # Handles the case where the string doesn't match the date format
        return None


# STEP 1: Get all U.S. zip codes and their lat/lon centroids
# Source: https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2022_Gazetteer/2022_Gaz_zcta_national.zip
raw_zips = (
    pd.read_csv(
        './data_raw/2022_Gaz_zcta_national.txt', 
        sep='\t',
        dtype={'GEOID': str}
    )
    .drop(columns=['ALAND', 'AWATER', 'ALAND_SQMI', 'AWATER_SQMI'])
    .rename(columns=lambda x: 'zipcode' if x == 'GEOID' else ('latitude' if x == 'INTPTLAT' else 'longitude'))
)

# STEP 2: Read in the NOAA station-level metadata
# Source (Data): https://www.ncei.noaa.gov/data/normals-daily/1991-2020/archive/us-climate-normals_1991-2020_v1.0.1_daily_temperature_by-variable_c20230403.tar.gz

raw_stations = (
    pd.read_csv(
        './data_built/station_normals.csv', 
        skiprows=1, 
        names=['station_id', 'latitude','longitude','altitude','station_name', 'last_freeze_meas_flag', 'last_freeze', 'first_freeze_meas_flag', 'first_freeze', 'avg_yrly_temp']
        )
    .dropna()
    .reset_index(drop=True)
)

# STEP 3: Merge on the closest NOAA station to each ZIP code using K-Nearest Neighbors algorithm, much faster than cartesian product

nn = NearestNeighbors(n_neighbors=1, algorithm='ball_tree').fit(raw_stations[['latitude', 'longitude']])
distances, indices = nn.kneighbors(raw_zips[['latitude', 'longitude']])
cleaned_freeze_data = (
    raw_zips
    .copy()
    .assign(
        station_id = raw_stations.loc[indices.flatten()]['station_id'].values,
        station_name = raw_stations.loc[indices.flatten()]['station_name'].values,
        station_altitude = raw_stations.loc[indices.flatten()]['altitude'].values,
        station_lat = raw_stations.loc[indices.flatten()]['latitude'].values,
        station_lon = raw_stations.loc[indices.flatten()]['longitude'].values,
        distance_miles = lambda df: np.sqrt(
            ((df['latitude'] - df['station_lat']) * 69)**2 +
            ((df['longitude'] - df['station_lon']) * 54.6)**2),
        last_freeze = raw_stations.loc[indices.flatten()]['last_freeze'].values,
        first_freeze = raw_stations.loc[indices.flatten()]['first_freeze'].values,
        avg_yrly_temp = raw_stations.loc[indices.flatten()]['avg_yrly_temp'].values,
        )
    .pipe(split_station_info)
)

# STEP 4: Read in the 1991-2020 'daily normals' dataset
# Source (Data): https://www.ncei.noaa.gov/data/normals-daily/1991-2020/archive/us-climate-normals_1991-2020_v1.0.1_daily_temperature_by-variable_c20230403.tar.gz
# Source (Methods): https://www.ncei.noaa.gov/data/normals-daily/1991-2020/doc/Normals_Calculation_Methodology_2020.pdf

raw_temps = (
    pd.read_csv('./data_raw/dly-temp-normal.csv')
    .drop_duplicates()
    .rename(columns={'GHCN_ID':'station_id'})
    .reset_index(drop=True)
    .rename(columns={'DLY-TMIN-NORMAL' : 'min_temp'})
    [['station_id', 'month', 'day', 'min_temp']]
)

freeze_results = (
    raw_temps
    .copy()
    .groupby('station_id')
    .apply(freeze_days).reset_index()
)

# Merge raw_temps with freeze_results to get cleaned_temps
station_frost_capacity = (
    raw_temps
    .copy()
    .merge(freeze_results, on='station_id', how='left')
    [['station_id', 'last_freeze', 'last_freeze_min_temp', 'first_freeze', 'first_freeze_min_temp']]
    .drop_duplicates()
    .reset_index(drop=True)
    .pipe(lambda df: df[df['last_freeze'] == "No frost / very infrequent"])
    [['station_id', 'last_freeze']]
    .rename(columns={'last_freeze':'frost_capacity'})
)

# STEP 5: Merge on the station-level freeze data for each ZIP code

raw_output = (
    cleaned_freeze_data
    .copy()
    .merge(station_frost_capacity, how='left', on='station_id')
    .sort_values('avg_yrly_temp')
    .assign(
        last_freeze = lambda df: df.last_freeze.str.strip(),
        first_freeze = lambda df: df.first_freeze.str.strip()
    )
    .assign(
        frost_capacity=lambda df: np.select(
            [
                df.avg_yrly_temp < 50,
                df.avg_yrly_temp >= 50
            ],
            [
                'year-round risk',
                'infrequent frost'
            ],
            default=df.frost_capacity
        ),
        last_freeze = lambda df: np.select(
            [
                (df.last_freeze == '-9999.0') & 
                (df.frost_capacity == 'year-round risk'),
                (df.last_freeze == '-9999.0') & 
                (df.frost_capacity == 'infrequent frost'),
            ],
            ['year-round risk', 'infrequent frost'],
            default=df.last_freeze.str.strip()
        ),
        first_freeze = lambda df: np.select(
            [
                (df.first_freeze == '-9999.0') & 
                (df.frost_capacity == 'year-round risk'),
                (df.first_freeze == '-9999.0') & 
                (df.frost_capacity == 'infrequent frost'),
            ],
            ['year-round risk', 'infrequent frost'],
            default=df.first_freeze.str.strip()
        ),
        station_altitude = lambda df: round(df.station_altitude*3.28),
        distance_miles = lambda df: round(df.distance_miles, 1)
    )
    .assign(
        last_freeze = lambda df: df.last_freeze.apply(format_date),
        first_freeze = lambda df: df.first_freeze.apply(format_date),
        growing_days = lambda df: df.apply(lambda row: date_difference(row['last_freeze'], row['first_freeze']), axis=1)
    )
    .assign(
        growing_days = lambda df: np.select(
            [
                (df.last_freeze == 'year-round risk'),
                (df.last_freeze == 'infrequent frost'),
            ],
            [0, 365],
            default=df.growing_days
        ),

    )
)

# STEP 6: Format output for delivery
cleaned_output = (
    raw_output
    .copy()
    [['zipcode', 'state_province', 'country', 'station_name', 'station_altitude', 'distance_miles', 'last_freeze', 'first_freeze', 'growing_days']]
    .rename(columns={'distance_miles': 'station_distance_miles'})
    .sort_values('zipcode')
    .assign(
        station_altitude = lambda df: df.station_altitude.astype(int),
        growing_days = lambda df: df.growing_days.astype(int)
    )
)

# STEP 7: Export cleaned output
cleaned_output.to_csv('./data_output/frost_tool_dict.csv',index=False)
cleaned_output.to_json('./data_output/frost_tool_dict.json', orient='records')