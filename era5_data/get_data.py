import cdsapi
from pathlib import Path

# ===============================
# CONFIGURATION – EASY TO MODIFY
# ===============================
years = range(2020, 2026)  # From 2024 to 2025 inclusive
months = range(1, 13)      # January to December
area = [71.5, -179.1, 18.9, -66.9]  # US bounding box: [N, W, S, E]
output_dir = Path("era5_downloads")
output_dir.mkdir(exist_ok=True)

variables = [
    "10m_u_component_of_wind",       # East-west wind at 10 meters above ground (positive = wind blowing east)
    "10m_v_component_of_wind",       # North-south wind at 10 meters above ground (positive = wind blowing north)
    "2m_dewpoint_temperature",       # Temperature at which air near the ground becomes saturated (humidity info)
    "2m_temperature",                # Air temperature measured 2 meters above the ground
    "total_precipitation",          # Total rain/snowfall during the hour leading up to the period (in meters of water)
    "mean_sea_level_pressure",       # Atmospheric pressure adjusted to sea level (used for tracking weather systems)
    "surface_pressure"              # Actual air pressure at ground level (depends on elevation and weather)
]

# ===============================
# Download loop
# ===============================
client = cdsapi.Client()

for year in years:
    for month in months:
        # Zero-padded strings
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
            "format": "netcdf",
            "area": area,
        }

        filename = output_dir / f"era5_us_{year_str}_{month_str}.nc"
        print(f"Downloading ERA5 data for {year_str}-{month_str}...")

        try:
            client.retrieve("reanalysis-era5-single-levels", request, str(filename))
            print(f"✔ Saved to {filename}")
        except Exception as e:
            print(f"✘ Failed for {year_str}-{month_str}: {e}")


