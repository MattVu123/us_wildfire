import cdsapi
from pathlib import Path
import zipfile
import xarray as xr

# ===============================
# CONFIGURATION
# ===============================
years = range(2020, 2026)
months = range(1, 13)
area = [71.5, -179.1, 18.9, -66.9]  # [N, W, S, E]
output_dir = Path.cwd() / "era5_daily_downloads"
output_dir.mkdir(exist_ok=True)

# Separate variables
# Instantaneous variables – use daily_statistic: "daily_mean"
instant_vars = [
    "10m_u_component_of_wind", # Zonal (west–east) wind at 10m [m/s]
    "10m_v_component_of_wind", # Meridional (south–north) wind at 10m [m/s]
    "2m_dewpoint_temperature", # Dewpoint temp at 2m [K]
    "2m_temperature", # Air temperature at 2m [K] 
    "mean_sea_level_pressure", # Atmspheric pressure reduced to sea level [Pa]
    "surface_pressure", # Atmospheric pressure at surface [Pa]
    "leaf_area_index_high_vegetation", # Leaf area index for high vegetation [dimensionless]
    "leaf_area_index_low_vegetation", # Leaf area index for low vegetation [dimensionless]
]

# Accumulated variables – use daily_statistic: "daily_sum"
accum_vars = [
    "total_precipitation", # Total precipitation [m]
    "surface_net_solar_radiation", # Net shortwave radiation at surface [J/m²]
]

# ===============================
# DOWNLOAD, EXTRACT, MERGE, CLEANUP
# ===============================
client = cdsapi.Client()

def download_and_extract(var_list, stat, year, month, suffix):
    year_str = str(year)
    month_str = f"{month:02d}"
    days = [f"{day:02d}" for day in range(1, 32)]

    request = {
        "product_type": "reanalysis",
        "variable": var_list,
        "year": year_str,
        "month": month_str,
        "day": days,
        "daily_statistic": stat,
        "time_zone": "utc+00:00",
        "frequency": "1_hourly",
        "data_format": "netcdf",
        "download_format": "zip",
        "area": area,
    }

    zip_filename = output_dir / f"era5_{suffix}_{year_str}_{month_str}.zip"

    print(f"\nRequesting {suffix} data for {year_str}-{month_str}...")
    client.retrieve("derived-era5-single-levels-daily-statistics", request, str(zip_filename))
    print(f"Saved {zip_filename.name}")

    with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
        nc_files = [f for f in zip_ref.namelist() if f.endswith(".nc")]
        zip_ref.extractall(path=output_dir)

    return nc_files, zip_filename


for year in years:
    for month in months:
        try:
            # Download and extract both types
            instant_files, zip_instant = download_and_extract(instant_vars, "daily_mean", year, month, "instant")
            accum_files, zip_accum = download_and_extract(accum_vars, "daily_sum", year, month, "accum")

            # Load all NetCDFs
            print(f"Merging datasets for {year}-{month:02d}...")
            all_files = instant_files + accum_files
            datasets = [xr.open_dataset(output_dir / f) for f in all_files]
            ds_merged = xr.merge(datasets)

            # Save merged dataset
            merged_path = output_dir / f"era5_us_{year}_{month:02d}.nc"
            ds_merged.to_netcdf(merged_path)
            print(f"Saved merged dataset to {merged_path.name}")

            # Close datasets
            for ds in datasets:
                ds.close()

            # Clean up
            for f in all_files:
                file_path = output_dir / f
                if file_path.exists():
                    file_path.unlink()
                    print(f"Deleted {file_path.name}")
            zip_instant.unlink()
            zip_accum.unlink()
            print("Deleted zip files")

        except Exception as e:
            print(f"Failed for {year}-{month:02d}: {e}")
