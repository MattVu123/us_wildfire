import pandas as pd
import geopandas as gpd
from pathlib import Path

# Paths (update as needed)
wildfire_perim_dir = Path(r'C:\Users\Azeem\Documents\MS\FInal Proj\Data\Wildfire Atlas\shp')
us_boundary_fp = Path(r'C:\Users\Azeem\Documents\MS\FInal Proj\Data\us boundary\cb_2023_us_nation_20m.shp')
intermediate_csv = (r'C:\Users\Azeem\Documents\MS\FInal Proj\Data\Wildfire Atlas\BurnData.csv')
output_csv = Path(r'C:\Users\Azeem\Documents\MS\FInal Proj\Data\Wildfire Atlas\BurnData_revised.csv')
gaccpath = Path(r"C:\Users\Azeem\Documents\MS\FInal Proj\Data\GACC\National_GACC_Final_20250113.shp")

# Helper function to get season from month
def get_season(month):
    if month in [3, 4, 5]:
        return 'Spring'
    elif month in [6, 7, 8]:
        return 'Summer'
    elif month in [9, 10, 11]:
        return 'Fall'
    else:
        return 'Winter'

def create_burn_data(shp_dir, us_boundary_fp):
    shp_list = list(shp_dir.glob('*.shp'))
    us_bound = gpd.read_file(str(us_boundary_fp),engine='pyogrio')
    us_bound_proj = us_bound.to_crs("EPSG:4326")

    all_rows = []
    columns = ['unique_id', 'size (acres)', 'perimiter (miles)', 'day', 'month', 'year',
               'startdateseason', 'enddateday', 'enddatemonth', 'enddateyear', 'enddatesesason', 'duration',
               'fire_line (miles)', 'fire_spread (acres/day)', 'fire_speed (miles/day)', 'dominant_direction', 'geometry']

    for shp_path in shp_list:
        year_str = shp_path.stem[-4:]  
        print(f"Processing year {year_str}")

        gdf = gpd.read_file(str(shp_path),engine='pyogrio')
        gdf['geometry'] = gdf.geometry.make_valid()
        gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notnull()]

        # Filter by size, start_date and end_date presence
        gdf = gdf[gdf['size'].notna() & (gdf['size'] >= 4.04686)]  # Size >= 1 acre in hectares
        gdf = gdf[gdf['start_date'].notna() & gdf['end_date'].notna()]

        # Convert date columns to datetime
        gdf['start_date'] = pd.to_datetime(gdf['start_date'])
        gdf['end_date'] = pd.to_datetime(gdf['end_date'])

        # Extract date parts
        gdf['startdateday'] = gdf['start_date'].dt.day
        gdf['startdatemonth'] = gdf['start_date'].dt.month
        gdf['startdateyear'] = gdf['start_date'].dt.year
        gdf['startdateseason'] = gdf['startdatemonth'].map(get_season)

        gdf['enddateday'] = gdf['end_date'].dt.day
        gdf['enddatemonth'] = gdf['end_date'].dt.month
        gdf['enddateyear'] = gdf['end_date'].dt.year
        gdf['enddatesesason'] = gdf['enddatemonth'].map(get_season)
        print('Iterrating Rows')

        for _, row in gdf.iterrows():
            geom = row.geometry

            # Skip if geometry is empty or not within US boundary polygon
            if geom.is_empty or not geom.within(us_bound_proj.unary_union):
                continue

            # Only keep fires starting and ending in the same year as filename
            if row['startdateyear'] != int(year_str) or row['enddateyear'] != int(year_str):
                continue

            unique_id = f"{year_str}_{row['fire_ID']}"
            size_acres = row['size'] * 247.105  
            perimeter_miles = row['perimeter'] * 0.621371  # km to miles
            fire_line_miles = row['fire_line'] * 0.621371 if pd.notna(row['fire_line']) else None
            fire_spread_acres_day = row['spread'] * 247.105 if pd.notna(row['spread']) else None
            fire_speed_miles_day = row['speed'] * 0.621371 if pd.notna(row['speed']) else None

            all_rows.append([
                unique_id,
                size_acres,
                perimeter_miles,
                row['startdateday'],
                row['startdatemonth'],
                row['startdateyear'],
                row['startdateseason'],
                row['enddateday'],
                row['enddatemonth'],
                row['enddateyear'],
                row['enddatesesason'],
                row['duration'],
                fire_line_miles,
                fire_spread_acres_day,
                fire_speed_miles_day,
                row['direction'],
                geom.wkt
            ])

    return columns, all_rows

def write_csv(columns, rows, out_fp):
    import csv
    with open(out_fp, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)
    print(f"Saved CSV to {out_fp}")

if __name__ == "__main__":
    cols, data = create_burn_data(wildfire_perim_dir, us_boundary_fp)
    write_csv(cols, data, intermediate_csv)

def add_gacc_info(csv,gacc_path):
    from shapely import wkt
    burndf = pd.read_csv(csv)
    burndf['geometry'] = burndf['geometry'].apply(wkt.loads)
    bgdf = gpd.GeoDataFrame(burndf, geometry='geometry', crs='EPSG:4326')
    bgdf_proj = bgdf.to_crs("EPSG:5070")
    gacc = gpd.read_file(str(gacc_path))
    gacc_proj = gacc.to_crs("EPSG:5070")
    gacc_proj ["GACC_AREA"] = gacc_proj.geometry.area * 0.000247105
    gacc_joined_gdf = gpd.sjoin_nearest(bgdf_proj,gacc_proj["GACCName","Area_acres","geometry"])
    gacc_joined_gdf = gacc_joined_gdf.drop(columsn='geometry_right')
    gacc_joined_gdf = gacc_joined_gdf.to_crs("EPSG:4326")
    gacc_joined_gdf.to_csv(output_csv)

add_gacc_info(intermediate_csv,gaccpath)


#%%

# %%

# Calculate mode
