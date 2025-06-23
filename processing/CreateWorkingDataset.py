#%%
import geopandas as gpd
import pandas as pd
import pathlib
import xarray as xr
from shapely import wkt

# Setup paths and variables
burndatapath = pathlib.Path(r"C:\Users\Azeem\Documents\MS\FInal Proj\Data\Wildfire Atlas\BurnData.csv")
outputpath = pathlib.Path(r"C:\Users\Azeem\Documents\MS\FInal Proj\Data\Working Dataset")
folderpath = pathlib.Path(r"C:\Users\Azeem\Documents\MS\FInal Proj\Data\era5_daily_downloads")


# Preload and prepare burn data
burndataframe = pd.read_csv(str(burndatapath))
burndataframe['geometry'] = burndataframe['geometry'].apply(wkt.loads)
#projected crs to grb centroid
bgdf = gpd.GeoDataFrame(burndataframe, geometry='geometry', crs='EPSG:4326')
bgdf = bgdf.to_crs('EPSG:2163')
bgdf['geometry'] = bgdf['geometry'].representative_point()


#%%
def process_climatedata(burngeodataframe):
    print('Processing Climate Data and matching to burn data')
    filelist = [file for file in folderpath.glob('*.nc')]
    burngeodataframe['burn_start'] = pd.to_datetime(burngeodataframe['startdate'])
    burngeodataframe['burn_end'] = pd.to_datetime(burngeodataframe['enddate'])
    burngeodataframe.drop(columns = ['startdate','enddate'])
    all_matches = []
    for file in filelist:
        month = int(file.stem[-2:])
        year = int(file.stem[-7:-3])
        #goal is to map grab the lat lon closest to each centroid in the burndata that matches month and year
        timefilteredburndata = burngeodataframe[(burngeodataframe['burn_start'].dt.month == month) & (burngeodataframe['burn_end'].dt.month == month) & (burngeodataframe['burn_start'].dt.year == year) & (burngeodataframe['burn_end'].dt.year == year)]
        df = xr.open_dataset(file).to_dataframe().reset_index()
        #project to grab closest geom in bgdf
        gdf = gpd.GeoDataFrame(df, geometry=(gpd.points_from_xy(x=df['longitude'],y=df['latitude'])),crs='EPSG:4326')
        gdf['datetime'] = pd.to_datetime(gdf['valid_time'])
        gdf.drop(columns=['valid_time'])
        for _,row in timefilteredburndata.iterrows():
            start = row['burn_start']
            end = row['burn_end']
            subset = gdf[(gdf['datetime'] >= start) & (gdf['datetime'] <= end)]
            projected_gdf = subset.to_crs('EPSG:2163')
            fire_gdf = gpd.GeoDataFrame([row], geometry='geometry',crs=burngeodataframe.crs)
            matched_data = gpd.sjoin_nearest(left_df=fire_gdf,right_df=projected_gdf,how='left',distance_col='dist')
            all_matches.append(matched_data)
    return all_matches
    
    
processed_data = process_climatedata(bgdf)   
final_matches = pd.concat(processed_data, ignore_index=True)
final_matches.to_csv(outputpath / 'Wildfire_Weather_2020_2024.csv', index=False)



