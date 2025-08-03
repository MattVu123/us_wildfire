import cdsapi
from pathlib import Path
import zipfile
import xarray as xr

# ===============================
# CONFIGURATION
# ===============================

YEARS = range(2020, 2026)
MONTHS = range(1, 13)
AREA = [71.5, -179.1, 18.9, -66.9]  # [N, W, S, E]
OUTPUT_DIR = Path(__file__).resolve().parents[2] / "data" / "raw" / "era5_daily"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

INSTANT_VARS = [
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "2m_dewpoint_temperature",
    "2m_temperature",
    "mean_sea_level_pressure",
    "surface_pressure",
    "leaf_area_index_high_vegetation",
    "leaf_area_index_low_vegetation",
]

ACCUM_VARS = [
    "total_precipitation",
    "surface_net_solar_radiation",
]

client = cdsapi.Client()

# ===============================
# FUNCTIONS
# ===============================

def download_and_extract(var_list, stat, year, month, suffix):
    days = [f"{day:02d}" for day in range(1, 32)]
    zip_filename = OUTPUT_DIR / f"era5_{suffix}_{year}_{month:02d}.zip"

    request = {
        "product_type": "reanalysis",
        "variable": var_list,
        "year": str(year),
        "month": f"{month:02d}",
        "day": days,
        "daily_statistic": stat,
        "time_zone": "utc+00:00",
        "frequency": "1_hourly",
        "data_format": "netcdf",
        "download_format": "zip",
        "area": AREA,
    }

    print(f"\nDownloading {suffix} for {year}-{month:02d}...")
    client.retrieve("derived-era5-single-levels-daily-statistics", request, str(zip_filename))

    with zipfile.ZipFile(zip_filename, "r") as zip_ref:
        nc_files = [f for f in zip_ref.namelist() if f.endswith(".nc")]
        zip_ref.extractall(path=OUTPUT_DIR)

    return nc_files, zip_filename

def merge_and_cleanup(nc_files, year, month, zip_instant, zip_accum):
    datasets = [xr.open_dataset(OUTPUT_DIR / f) for f in nc_files]
    merged = xr.merge(datasets)
    out_path = OUTPUT_DIR / f"era5_us_{year}_{month:02d}.nc"
    merged.to_netcdf(out_path)
    print(f"Saved merged: {out_path.name}")

    for ds in datasets:
        ds.close()

    # Delete temp NetCDFs and zip files
    for f in nc_files:
        try:
            (OUTPUT_DIR / f).unlink()
        except FileNotFoundError:
            pass
    zip_instant.unlink(missing_ok=True)
    zip_accum.unlink(missing_ok=True)

# ===============================
# PIPELINE
# ===============================

def run_download_pipeline():
    for year in YEARS:
        for month in MONTHS:
            try:
                inst_files, zip_inst = download_and_extract(INSTANT_VARS, "daily_mean", year, month, "instant")
                accum_files, zip_accum = download_and_extract(ACCUM_VARS, "daily_sum", year, month, "accum")

                merge_and_cleanup(inst_files + accum_files, year, month, zip_inst, zip_accum)

            except Exception as e:
                print(f"Failed {year}-{month:02d}: {e}")

if __name__ == "__main__":
    run_download_pipeline()
