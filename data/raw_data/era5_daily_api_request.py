import cdsapi
from pathlib import Path
import zipfile
import xarray as xr

# ===============================
# CONFIGURATION – EASY TO MODIFY
# ===============================
years = range(2020, 2026)  # From 2020 to 2025 inclusive
months = range(1, 13)      # January to December
area = [71.5, -179.1, 18.9, -66.9]  # US bounding box: [N, W, S, E]
output_dir = Path.cwd() / "era5_daily_downloads"
output_dir.mkdir(exist_ok=True)

variables = [
    # INSTANTANEOUS VARIABLES
    "10m_u_component_of_wind",         # Instant – daily (24h) average hourly Zonal (west–east) wind at 10m [m/s]
    "10m_v_component_of_wind",         # Instant – daily (24h) average hourly Meridional (south–north) wind at 10m [m/s]
    "2m_dewpoint_temperature",         # Instant – daily (24h) average hourly Dewpoint temp at 2m [K], convert: °C = K - 273.15
    "2m_temperature",                  # Instant – daily (24h) average hourly Air temperature at 2m [K], convert: °C = K - 273.15
    "mean_sea_level_pressure",         # Instant – daily (24h) average hourly Pressure reduced to sea level [Pa]
    "surface_pressure",                # Instant – daily (24h) average hourly Atmospheric pressure at surface [Pa]
    "leaf_area_index_high_vegetation", # Instant – daily (24h) average hourly LAI high vegetation [dimensionless]
    "leaf_area_index_low_vegetation",  # Instant – daily (24h) average hourly LAI low vegetation [dimensionless]

    # ACCUMULATED VARIABLES
    "total_precipitation",            # Accum – daily (24h) average hourly total precipitation over the previous hour [m]
    "surface_net_solar_radiation",    # Accum – daily (24h) average hourly net shortwave radiation at surface [J/m²]
]

# ===============================
# DOWNLOAD + EXTRACT + MERGE
# ===============================
client = cdsapi.Client()

for year in years:
    for month in months:
        year_str = str(year)
        month_str = str(month).zfill(2)
        days = [str(day).zfill(2) for day in range(1, 32)]  # CDS handles invalid dates internally

        request = {
            "product_type": "reanalysis",
            "variable": variables,
            "year": year_str,
            "month": month_str,
            "day": days,
            "daily_statistic": "daily_mean",
            "time_zone": "utc+00:00",
            "frequency": "1_hourly",
            "data_format": "netcdf",
            "download_format": "zip",
            "area": area,
        }

        zip_filename = output_dir / f"era5_us_{year_str}_{month_str}.zip"
        merged_path = output_dir / f"era5_us_{year_str}_{month_str}.nc"

        print(f"\nDownloading ERA5 data for {year_str}-{month_str}...")

        try:
            client.retrieve("derived-era5-single-levels-daily-statistics", request, str(zip_filename))
            print(f"Saved to {zip_filename.name}")

            # Extract all .nc files
            with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
                nc_files = [f for f in zip_ref.namelist() if f.endswith(".nc")]
                zip_ref.extractall(path=output_dir)

            # Load and merge datasets
            print("Merging NetCDF files...")
            datasets = [xr.open_dataset(output_dir / f) for f in nc_files]
            ds_merged = xr.merge(datasets)
            ds_merged.to_netcdf(merged_path)
            print(f"Merged dataset saved to {merged_path.name}")

            # Close all datasets
            for ds in datasets:
                ds.close()

            # Cleanup
            for f in nc_files:
                nc_path = output_dir / f
                if nc_path.exists():
                    nc_path.unlink()
                    print(f"Deleted {nc_path.name}")
            zip_filename.unlink()
            print(f"Deleted {zip_filename.name}")

        except Exception as e:
            print(f"Failed for {year_str}-{month_str}: {e}")




