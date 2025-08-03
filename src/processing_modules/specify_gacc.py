import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

GACC_SHAPEFILE_PATH = '../data/raw_data/gacc_boundaries/National_GACC_Final_20250113.shp'
GACC_GDF = gpd.read_file(GACC_SHAPEFILE_PATH)
GACC_GDF = GACC_GDF.rename(columns={'GACCName': 'gacc'})

def add_gacc_to_dataframe(df: pd.DataFrame, lon_col: str, lat_col: str) -> pd.DataFrame:
    df[lon_col] = pd.to_numeric(df[lon_col], errors='coerce')
    df[lat_col] = pd.to_numeric(df[lat_col], errors='coerce')
    df = df.dropna(subset=[lon_col, lat_col])

    geometry = [Point(xy) for xy in zip(df[lon_col], df[lat_col])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

    if gdf.crs != GACC_GDF.crs:
        gdf = gdf.to_crs(GACC_GDF.crs)

    gdf_with_gacc = gpd.sjoin(
        gdf,
        GACC_GDF[['geometry', 'gacc']],
        how='left',
        predicate='within',
        lsuffix='',
        rsuffix='_gacc'
    )
    return gdf_with_gacc.drop(columns='geometry')

def process_and_save_with_gacc(input_path: str, lon_col: str, lat_col: str, output_path: str, label: str) -> None:
    df = pd.read_csv(input_path)
    df_with_gacc = add_gacc_to_dataframe(df, lon_col=lon_col, lat_col=lat_col)
    df_with_gacc.to_csv(output_path, index=False)

    gacc_regions = df_with_gacc['gacc'].dropna().unique()
    print(f"{label} records tagged with {len(gacc_regions)} unique GACC regions.")
    print("Regions:", gacc_regions)
    print(f"Saved {label} data with GACC to: {output_path}")

def main():
    process_and_save_with_gacc(
        input_path='../data/processed_data/Wildfire_Weather_2020_2024.csv',
        lon_col='longitude',
        lat_col='latitude',
        output_path='../data/processed_data/Wildfire_Weather_2020_2024_with_gacc.csv',
        label='Wildfire weather'
    )

    process_and_save_with_gacc(
        input_path='../data/processed_data/airports_processed.csv',
        lon_col='longitude_deg',
        lat_col='latitude_deg',
        output_path='../data/processed_data/airports_processed.csv',
        label='Airports processed'
    )

    process_and_save_with_gacc(
        input_path='../data/processed_data/airports_runways_joined.csv',
        lon_col='longitude_deg',
        lat_col='latitude_deg',
        output_path='../data/processed_data/airports_runways_joined.csv',
        label='Airports runways joined'
    )

if __name__ == "__main__":
    main()
