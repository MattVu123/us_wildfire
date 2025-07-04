# #%%
import pandas as pd
import geopandas as gpd
#make sure that shpaefiles for wildfire atlas perimiters 2020-2024 are in a folder, shp files need .shp, .prj, .shx. and .dbf so extract all!
from pathlib import Path
wildfire_perim = Path(r'C:\Users\Azeem\Documents\MS\FInal Proj\Data\Wildfire Atlas\shp')
us_boundary = Path(r'C:\Users\Azeem\Documents\MS\FInal Proj\Data\us boundary\cb_2023_us_nation_20m.shp')
shp_list = list(wildfire_perim.glob('*.shp'))
#%%
def get_season(month):
    if month in [3, 4, 5]:
        return 'Spring'
    elif month in [6, 7, 8]:
        return 'Summer'
    elif month in [9, 10, 11]:
        return 'Fall'
    else:
        return 'Winter'

#%%
def createBurnData(shp_list:list[Path]):
    row_data = []
    for shp in shp_list:
        csv_fields = ['unique_id','size (km2)','perimiter (km)','startdateday','startdatemonth','startdateyear','startdateseason','enddateday','enddatemonth','enddateyear','enddatesesason','duration','fire_line (km)','fire_spread (km2/day)', 'fire_speed (km/day)', 'dominant_direction','geometry']
        year = str(shp)[-8:-4]
        print(f'evaluating {year} data')
        shp_gdf = gpd.read_file(shp)
        us_bound = gpd.read_file(us_boundary).to_crs(shp_gdf.crs)
        us_all = us_bound.union_all()
        #add make valid because of invalid geometry 
        shp_gdf['geometry'] = shp_gdf.geometry.make_valid()
        shp_gdf_check = shp_gdf[shp_gdf['size'].notna()]
        filtered_gdf = shp_gdf_check[(shp_gdf_check['size'] >= 4.04686) & (shp_gdf_check['start_date'].notna()) & (shp_gdf_check['end_date']).notna()]
        filtered_gdf['start_date'] = (pd.to_datetime(filtered_gdf['start_date']))
        filtered_gdf['startdateday'] = filtered_gdf['start_date'].dt.day
        filtered_gdf['startdatemonth'] = filtered_gdf['start_date'].dt.month
        filtered_gdf['startdateyear'] = filtered_gdf['start_date'].dt.year
        filtered_gdf['startseason'] = filtered_gdf['startdatemonth'].map(get_season)
        filtered_gdf['end_date'] = (pd.to_datetime(filtered_gdf['end_date']))
        filtered_gdf['enddateday'] = filtered_gdf['end_date'].dt.day
        filtered_gdf['enddatemonth'] = filtered_gdf['end_date'].dt.month
        filtered_gdf['enddateyear'] = filtered_gdf['end_date'].dt.year
        filtered_gdf['endseason'] = filtered_gdf['enddatemonth'].map(get_season)
        filtered_gdf.drop(['start_date','end_date'], axis=1)
        print(f'iterating through {year}')
        for _,values in filtered_gdf.iterrows():
            unique_id = f"{year}_{values['fire_ID']}"
            size = values['size']
            perim = values['perimeter']
            start_year = values['startdateyear']
            start_month = values['startdatemonth']
            start_day = values['startdateday']
            start_season = values['startseason']
            end_year = values['enddateyear']
            end_month = values['enddatemonth']
            end_day = values['enddateday']
            end_season = values['endseason']
            dur = values['duration']
            fireline = values['fire_line'] 
            spread = values['spread'] 
            speed = values['speed'] 
            direction = values['direction']
            geom = values['geometry']
            geom_string = values['geometry'].wkt
            #only grab fires within the year, dont repeat overlapping fires
            if start_year == int(year) and end_year == int(year) and geom.within(us_all) :
                row_data.append([unique_id, size, perim, start_day,start_month,start_year,start_season,end_day,end_month,end_year, end_season, dur, fireline, spread, speed, direction, geom_string])
    return csv_fields, row_data

csv_fields, info = createBurnData(shp_list)



def writecsv(csv_fields:list[str],row_data:list[tuple]):
    import csv
    with open(r'C:\Users\Azeem\Documents\MS\FInal Proj\Data\Wildfire Atlas\BurnData.csv','w' ,newline='') as w:
        writer = csv.writer(w)
        writer.writerow(csv_fields)
        for row in row_data:
            writer.writerow(row)
    print('Wrote CSV')
writecsv(csv_fields,info)



#%%

import pandas as pd
import matplotlib.pyplot as plt

sheet = pd.read_csv(r'C:\Users\Azeem\Documents\MS\FInal Proj\Data\Wildfire Atlas\BurnData.csv')

sheet['size (km2)'].describe()

# %%

# Calculate mode
mode_col1 = sheet['size (km2)'].mode()
print(f"Mode of col1: {mode_col1.values}")

# Calculate mean
mean_col1 = sheet['size (km2)'].mean()
print(f"Mean of col1: {mean_col1}")

# Calculate median
median_col1 = sheet['size (km2)'].median()
print(f"Median of col1: {median_col1}")

# Calculate standard deviation
std_col1 = sheet['size (km2)'].std()
print(f"Standard deviation of col1: {std_col1}")

# Calculate IQR
Q1_col1 = sheet['size (km2)'].quantile(0.25)
Q3_col1 = sheet['size (km2)'].quantile(0.75)
iqr_col1 = Q3_col1 - Q1_col1
print(f"IQR of col1: {iqr_col1}")


# Calculate min
min_col1 = sheet['size (km2)'].min()
print(f"Min of col1: {min_col1}")

# Calculate max
max_col1 = sheet['size (km2)'].max()
print(f"Max of col1: {max_col1}")


print('Check')