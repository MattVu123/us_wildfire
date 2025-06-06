import cdsapi
from pathlib import Path
import zipfile

# ===============================
# CONFIGURATION – EASY TO MODIFY
# ===============================
years = range(2020, 2026)  # From 2020 to 2025 inclusive
months = range(1, 13)      # January to December
area = [71.5, -179.1, 18.9, -66.9]  # US bounding box: [N, W, S, E]
output_dir = Path.cwd() / "era5_downloads"
output_dir.mkdir(exist_ok=True)

variables = [
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "2m_dewpoint_temperature",
    "2m_temperature",
    "total_precipitation",
    "mean_sea_level_pressure",
    "surface_pressure"
]

# ===============================
# Download + Extract loop
# ===============================
client = cdsapi.Client()

for year in years:
    for month in months:
        year_str = str(year)
        month_str = str(month).zfill(2)
        days = [str(day).zfill(2) for day in range(1, 32)]  # CDS handles invalid days

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
        print(f"Downloading ERA5 data for {year_str}-{month_str}...")

        try:
            client.retrieve("reanalysis-era5-single-levels", request, str(zip_filename))
            print(f"✔ Saved to {zip_filename}")

            # Extract only the data_stream-oper_stepType-instant.nc file
            with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
                target_file = "data_stream-oper_stepType-instant.nc"
                if target_file in zip_ref.namelist():
                    print(f"Extracting {target_file}...")
                    zip_ref.extract(target_file, path=output_dir)
                    extracted_path = output_dir / target_file

                    # Rename extracted file
                    new_nc_filename = output_dir / f"era5_us_{year_str}_{month_str}.nc"
                    extracted_path.rename(new_nc_filename)
                    print(f"✔ Extracted and renamed to {new_nc_filename}")
                else:
                    print(f"⚠️ {target_file} not found in zip {zip_filename.name}")

            # Remove the zip file after extraction
            zip_filename.unlink()
            print(f"Deleted zip file {zip_filename.name}")

        except Exception as e:
            print(f"✘ Failed for {year_str}-{month_str}: {e}")


