# airport_data_processing.py
# ------------------------------------------------------------------
# Script for processing US airport and runway data for wildfire risk
# ------------------------------------------------------------------

import os
import pandas as pd
import pdfplumber
import re

# Constants
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
DATA_RAW = os.path.join(BASE_DIR, 'data', 'raw', 'our_airports_raw')
DATA_PROCESSED = os.path.join(BASE_DIR, 'data', 'processed_data')
RAW_DATA = os.path.join(BASE_DIR, 'data', 'raw_data')


# Utility function for reading CSVs
def read_csv(filepath, rename_dict=None):
    df = pd.read_csv(filepath, encoding='utf-8')
    if rename_dict:
        df = df.rename(columns=rename_dict)
    return df


def load_airport_data():
    airports = read_csv(
        os.path.join(DATA_RAW, 'airports.csv'),
        rename_dict={'id': 'airport_id'}
    )
    runways = read_csv(
        os.path.join(DATA_RAW, 'runways.csv'),
        rename_dict={'id': 'runways_id'}
    )
    countries = read_csv(
        os.path.join(DATA_RAW, 'countries.csv'),
        rename_dict={'id': 'countries_id'}
    )
    regions = read_csv(
        os.path.join(DATA_RAW, 'regions.csv'),
        rename_dict={'id': 'regions_id'}
    )
    return airports, runways, countries, regions


def join_airport_data(airports, runways, countries, regions):
    df = pd.merge(airports, runways, left_on='ident', right_on='airport_ident', how='left')
    df = pd.merge(df, countries[['code', 'name']], left_on='iso_country', right_on='code', how='left', suffixes=('', '_country'))
    df = df.rename(columns={'name_country': 'country_name'})
    df = pd.merge(df, regions[['code', 'name']], left_on='iso_region', right_on='code', how='left', suffixes=('', '_region'))
    df = df.rename(columns={'name_region': 'region_name'})
    return df


def filter_airports(df):
    valid_types = ['large_airport', 'medium_airport', 'small_airport']
    df = df[df['type'].isin(valid_types)].copy()

    pattern = '|'.join(['asp', 'conc', 'groov', 'tar', 'tarmac', 'cem', 'pav'])
    df = df[df['surface'].str.lower().str.contains(pattern, na=False)]

    def standardize_surface(surface):
        s = surface.lower()
        if any(sub in s for sub in ['asp', 'pav']):
            return 'asphalt'
        elif any(sub in s for sub in ['conc', 'groov', 'cem']):
            return 'grooved concrete'
        elif any(sub in s for sub in ['tar', 'tarmac']):
            return 'tarmac'
        return s

    df['surface'] = df['surface'].apply(standardize_surface)
    df = df[df['length_ft'] >= 5000]

    valid_regions = set([...])  # Include your CONUS, Alaska, Hawaii states here

    df = df[
        (df['country_name'].str.strip().str.lower() == 'united states') &
        (df['region_name'].str.strip().str.lower().isin(valid_regions))
    ]

    return df


def filter_npias_military(df):
    npias = pd.read_excel(os.path.join(RAW_DATA, 'npias.xlsx'), sheet_name="All NPIAS Airports")
    military = pd.read_excel(os.path.join(RAW_DATA, 'military_airports.xlsx'))

    valid_codes = set(npias['LocID'].dropna().str.lower()).union(
        set(military['ICAO or FAA LID'].dropna().str.lower())
    )

    matches = (
        df['ident'].astype(str).str.lower().isin(valid_codes) |
        df['iata_code'].astype(str).str.lower().isin(valid_codes) |
        df['icao_code'].astype(str).str.lower().isin(valid_codes) |
        df['local_code'].astype(str).str.lower().isin(valid_codes)
    )

    return df[matches]


def aggregate_runways(df):
    df_sorted = df.sort_values(['ident', 'length_ft'])

    def agg(group):
        lengths = group['length_ft'].dropna().astype(int).astype(str).tolist()
        surfaces = group['surface'].fillna('').astype(str).tolist()
        return pd.Series({
            'runway_lengths_ft': ','.join(lengths),
            'runway_surfaces': ','.join(surfaces[:len(lengths)])
        })

    aggregated = df_sorted.groupby('ident').apply(agg).reset_index()
    df_single = df_sorted.drop_duplicates('ident').drop(columns=['length_ft', 'surface'])
    return df_single.merge(aggregated, on='ident', how='left')


def flag_airtanker_bases(df):
    pdf_path = os.path.join(RAW_DATA, 'pms507-ATB-directory2018.pdf')
    names = set()
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                match = re.search(r"^(.*?)\u2013Elevation", text, re.MULTILINE)
                if match:
                    names.add(match.group(1).strip().lower())

    df['airtanker_base'] = df['name'].astype(str).str.strip().str.lower().isin(names)
    return df


def save_outputs(df):
    df.to_csv(os.path.join(DATA_PROCESSED, 'airports_runways_joined.csv'), index=False)

    airports_only = df.drop(columns=['runway_lengths_ft', 'runway_surfaces'], errors='ignore')
    airports_only = airports_only[[
        'ident', 'iata_code', 'icao_code', 'local_code', 'name', 'type',
        'latitude_deg', 'longitude_deg', 'elevation_ft', 'country_name',
        'region_name', 'airtanker_base']]
    airports_only.to_csv(os.path.join(DATA_PROCESSED, 'airports_processed.csv'), index=False)

    # Runway information
    runways_df = df[['ident', 'runway_lengths_ft', 'runway_surfaces']].copy()
    runways_df = runways_df.explode(['runway_lengths_ft', 'runway_surfaces'])  # optional if needed row-wise
    runways_df.to_csv(os.path.join(DATA_PROCESSED, 'runways_processed.csv'), index=False)


if __name__ == '__main__':
    airports, runways, countries, regions = load_airport_data()
    merged = join_airport_data(airports, runways, countries, regions)
    filtered = filter_airports(merged)
    filtered = filter_npias_military(filtered)
    filtered = aggregate_runways(filtered)
    filtered = flag_airtanker_bases(filtered)
    save_outputs(filtered)
    print("✅ Data processing complete.")
