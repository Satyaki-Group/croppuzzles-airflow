import io
import os
import zipfile
from datetime import datetime

import requests

BASE_URL = "https://apps.fas.usda.gov/psdonline/downloads/archives"

# CSV files to extract
ARCHIVES = {
    "psd_alldata_csv.zip": "psd_alldata.csv",
    "psd_grains_pulses_csv.zip": "psd_grains_pulses.csv",
    "psd_oilseeds_csv.zip": "psd_oilseeds.csv",
}

# Output folder for saved CSV files
OUTPUT_FOLDER = "sample_data"


def download_and_extract_one_month(year: int, month: int):
    """
    Download data for one specific month and extract CSV files locally
    """
    # Create output folder if it doesn't exist
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
        print(f"Created folder: {OUTPUT_FOLDER}")

    print(f"\nDownloading data for {year}/{month:02d}...")

    for archive_name, csv_name in ARCHIVES.items():
        url = f"{BASE_URL}/{year}/{month:02d}/{archive_name}"
        print(f"\nDownloading {url} ...", end=" ")

        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
        except requests.HTTPError as e:
            print(f"HTTP {response.status_code} – skipping")
            continue
        except requests.RequestException as e:
            print(f"ERROR: {e} – skipping")
            continue

        # Extract CSV from zip
        try:
            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                if csv_name not in zf.namelist():
                    print(f"'{csv_name}' not found in archive – skipping")
                    continue
                csv_bytes = zf.read(csv_name)
        except zipfile.BadZipFile:
            print("bad zip – skipping")
            continue

        # Save CSV locally
        local_path = os.path.join(OUTPUT_FOLDER, f"{year}{month:02d}_{csv_name}")
        with open(local_path, "wb") as f:
            f.write(csv_bytes)

        print(f"✓ Saved to {local_path}")

    print(f"\nDone! Files saved to {OUTPUT_FOLDER}/")


if __name__ == "__main__":
    # Download one month of data (change year and month as needed)
    year = 2024
    month = 3  # March
    
    download_and_extract_one_month(year, month)
