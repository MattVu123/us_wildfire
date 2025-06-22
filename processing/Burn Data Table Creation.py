#%%
import pandas as pd
import geopandas as gpd
#make sure that shpaefiles for wildfire atlas perimiters 2020-2024 are in a folder, shp files need .shp, .prj, .shx. and .dbf so extract all!
from pathlib import Path
wildfire_perim = Path(r'C:\Users\Azeem\Documents\MS\FInal Proj\Data\Wildfire Atlas\shp')
us_boundary = Path(r'C:\Users\Azeem\Documents\MS\FInal Proj\Data\us boundary\cb_2023_us_nation_20m.shp')
shp_list = list(wildfire_perim.glob('*.shp'))

#%%
def createBurnData(shp_list:list[Path]):
    row_data = []
    for shp in shp_list:
        csv_fields = ['unique_id','size (km2)','perimiter (km)','startdate','enddate','duration','fire_line (km)','fire_spread (km2/day)', 'fire_speed (km/day)', 'dominant_direction','geometry']
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
        filtered_gdf['end_date'] = (pd.to_datetime(filtered_gdf['end_date']))
        print(f'iterating through {year}')
        for _,values in filtered_gdf.iterrows():
            unique_id = f"{year}_{values['fire_ID']}"
            size = values['size']
            perim = values['perimeter']
            start = values['start_date']
            end = values['end_date']
            dur = values['duration']
            fireline = values['fire_line'] 
            spread = values['spread'] 
            speed = values['speed'] 
            direction = values['direction']
            geom = values['geometry']
            #only grab fires within the year, dont repeat overlapping fires
            if start.year == int(year) and end.year == int(year) and geom.within(us_all) :
                row_data.append([unique_id, size, perim, start, end, dur, fireline, spread, speed, direction, geom])
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
writecsv(info,csv_fields)








