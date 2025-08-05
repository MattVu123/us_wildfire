# the module contains functions to compute the distances between wildfires and the nearest airports/airtanker bases.
import pandas as pd
import numpy as np

WILDFIRE_DATA_PATH = "../data/processed_data/Wildfire_Weather_2020_2024_with_gacc.csv"
AIRPORT_DATA_PATH = "../data/processed_data/airports_runways_joined.csv"
OUTPUT_PATH = "../data/processed_data/nearest_airport_airtanker_bases_to_fires_final.csv"

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2)**2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2)**2
    return 2 * R * np.arcsin(np.sqrt(a)) * 1000

def prepare_data():
    wildfire_df = pd.read_csv(WILDFIRE_DATA_PATH)
    airport_df = pd.read_csv(AIRPORT_DATA_PATH)

    wildfire_df = wildfire_df.rename(columns={
        'startdateyear': 'year',
        'startdatemonth': 'month',
        'startdateday': 'day'
    })
    wildfire_df['startdate'] = pd.to_datetime(wildfire_df[['year', 'month', 'day']], errors='coerce')
    wildfire_df = wildfire_df.dropna(subset=['latitude', 'longitude', 'gacc'])
    wildfire_df['centroid_lat'] = wildfire_df['latitude']
    wildfire_df['centroid_lon'] = wildfire_df['longitude']

    airport_df = airport_df.dropna(subset=['latitude_deg', 'longitude_deg', 'gacc'])
    airport_df['airtanker_base'] = airport_df['airtanker_base'].astype(bool)

    return wildfire_df, airport_df

def find_nearest_airports(wildfire_df, airport_df):
    results = []
    for _, fire in wildfire_df.iterrows():
        lat1, lon1 = fire['centroid_lat'], fire['centroid_lon']
        fire_gacc = fire['gacc']

        airports_in_gacc = airport_df[airport_df['gacc'] == fire_gacc]
        if airports_in_gacc.empty:
            continue

        all_distances = haversine(lat1, lon1, airports_in_gacc['latitude_deg'], airports_in_gacc['longitude_deg'])
        idx_airport = np.argmin(all_distances)
        nearest_airport = airports_in_gacc.iloc[idx_airport]

        results.append({
            'fire_id': fire['unique_id'],
            'fire_lat': lat1,
            'fire_lon': lon1,
            'startdate': fire['startdate'],
            'duration': fire['duration'],
            'size (acres)': fire['size (acres)'],
            'fire_spread (acres/day)': fire['fire_spread (acres/day)'],
            'gacc': fire_gacc,
            'distance_nm': all_distances[idx_airport] / 1852,
            'ident': nearest_airport['ident'],
            'iata_code': nearest_airport['iata_code'],
            'icao_code': nearest_airport['icao_code'],
            'local_code': nearest_airport['local_code'],
            'closet_airport_name': nearest_airport['name'],
            'type': nearest_airport['type'],
            'latitude_deg': nearest_airport['latitude_deg'],
            'longitude_deg': nearest_airport['longitude_deg'],
            'elevation_ft': nearest_airport['elevation_ft'],
            'country_name': nearest_airport['country_name'],
            'region_name': nearest_airport['region_name'],
            'runway_lengths_ft': nearest_airport['runway_lengths_ft'],
            'runway_surfaces': nearest_airport['runway_surfaces'],
            'airtanker_base': bool(nearest_airport['airtanker_base'])
        })

        airtanker_df = airports_in_gacc[airports_in_gacc['airtanker_base'] == True]
        if not airtanker_df.empty:
            base_distances = haversine(lat1, lon1, airtanker_df['latitude_deg'], airtanker_df['longitude_deg'])
            idx_base = np.argmin(base_distances)
            nearest_base = airtanker_df.iloc[idx_base]

            results.append({
                'fire_id': fire['unique_id'],
                'fire_lat': lat1,
                'fire_lon': lon1,
                'startdate': fire['startdate'],
                'duration': fire['duration'],
                'size (acres)': fire['size (acres)'],
                'fire_spread (acres/day)': fire['fire_spread (acres/day)'],
                'gacc': fire_gacc,
                'distance_nm': base_distances[idx_base] / 1852,
                'ident': nearest_base['ident'],
                'iata_code': nearest_base['iata_code'],
                'icao_code': nearest_base['icao_code'],
                'local_code': nearest_base['local_code'],
                'closet_airport_name': nearest_base['name'],
                'type': nearest_base['type'],
                'latitude_deg': nearest_base['latitude_deg'],
                'longitude_deg': nearest_base['longitude_deg'],
                'elevation_ft': nearest_base['elevation_ft'],
                'country_name': nearest_base['country_name'],
                'region_name': nearest_base['region_name'],
                'runway_lengths_ft': nearest_base['runway_lengths_ft'],
                'runway_surfaces': nearest_base['runway_surfaces'],
                'airtanker_base': True
            })

    return pd.DataFrame(results)

def validate_results(df: pd.DataFrame, airport_df: pd.DataFrame):
    known_airtanker_bases = airport_df[airport_df['airtanker_base'] == True]['ident'].unique()
    incorrect_flags = df[(df['ident'].isin(known_airtanker_bases)) & (df['airtanker_base'] == False)]
    if not incorrect_flags.empty:
        print("Warning: The following airport records are known airtanker bases but marked False in results:")
        print(incorrect_flags[['fire_id', 'ident', 'closet_airport_name', 'airtanker_base']])
    else:
        print("All known airtanker bases correctly marked in results.")

def main():
    wildfire_df, airport_df = prepare_data()
    result_df = find_nearest_airports(wildfire_df, airport_df)
    result_df.to_csv(OUTPUT_PATH, index=False)
    print(result_df.head())
    validate_results(result_df, airport_df)

if __name__ == "__main__":
    main()
