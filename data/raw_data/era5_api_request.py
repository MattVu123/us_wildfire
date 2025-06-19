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
output_dir = Path.cwd() / "era5_downloads"
output_dir.mkdir(exist_ok=True)

variables = [
    # INSTANTANEOUS VARIABLES
    "10m_u_component_of_wind",         # Instant – Zonal (west–east) wind at 10m [m/s]
    "10m_v_component_of_wind",         # Instant – Meridional (south–north) wind at 10m [m/s]
    "2m_dewpoint_temperature",         # Instant – Dewpoint temp at 2m [K], convert: °C = K - 273.15
    "2m_temperature",                  # Instant – Air temperature at 2m [K], convert: °C = K - 273.15
    "mean_sea_level_pressure",         # Instant – Pressure reduced to sea level [Pa]
    "surface_pressure",                # Instant – Atmospheric pressure at surface [Pa]
    "leaf_area_index_high_vegetation", # Instant – LAI high vegetation [dimensionless]
    "leaf_area_index_low_vegetation",  # Instant – LAI low vegetation [dimensionless]

    # ACCUMULATED VARIABLES
    "total_precipitation",            # Accum – Total precipitation over previous hour [m]
    "surface_net_solar_radiation",    # Accum – Net shortwave radiation at surface [J/m²]
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
            "time": [f"{h:02d}:00" for h in range(24)],
            "data_format": "netcdf",
            "download_format": "zip",
            "area": area,
        }

        zip_filename = output_dir / f"era5_us_{year_str}_{month_str}.zip"
        print(f"\nDownloading ERA5 data for {year_str}-{month_str}...")

        try:
            client.retrieve("reanalysis-era5-single-levels", request, str(zip_filename))
            print(f"Saved to {zip_filename.name}")

            # Extract both instant and accum .nc files
            with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
                namelist = zip_ref.namelist()
                instant_file = next((f for f in namelist if "instant" in f), None)
                accum_file = next((f for f in namelist if "accum" in f), None)

                if not instant_file or not accum_file:
                    raise RuntimeError("Expected both 'instant' and 'accum' .nc files in zip archive")

                # Extract both files
                zip_ref.extract(instant_file, path=output_dir)
                zip_ref.extract(accum_file, path=output_dir)

            # Define paths
            instant_path = output_dir / instant_file
            accum_path = output_dir / accum_file
            renamed_instant = output_dir / f"era5_us_{year_str}_{month_str}_instant.nc"
            renamed_accum = output_dir / f"era5_us_{year_str}_{month_str}_accum.nc"
            instant_path.rename(renamed_instant)
            accum_path.rename(renamed_accum)

            # Merge datasets
            print("Merging instant and accum datasets...")
            ds_instant = xr.open_dataset(renamed_instant)
            ds_accum = xr.open_dataset(renamed_accum)
            ds_merged = xr.merge([ds_instant, ds_accum])

            merged_path = output_dir / f"era5_us_{year_str}_{month_str}.nc"
            ds_merged.to_netcdf(merged_path)
            print(f"Merged dataset saved to {merged_path.name}")

            # Close and cleanup
            ds_instant.close()
            ds_accum.close()
            for f in [zip_filename, renamed_instant, renamed_accum]:
                if f.exists():
                    f.unlink()
                    print(f"Deleted {f.name}")

        except Exception as e:
            print(f"Failed for {year_str}-{month_str}: {e}")




