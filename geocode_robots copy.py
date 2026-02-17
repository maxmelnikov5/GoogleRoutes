import os
import csv
import time
from typing import Optional, Tuple

import requests
import pandas as pd


def load_api_key() -> str:
    """
    Load Google Geocoding API key from .env or environment.

    Expects a variable named GOOGLE_MAPS_API_KEY either in:
    - OS environment, or
    - .env file in the project root (format: GOOGLE_MAPS_API_KEY=...).
    """
    # 1) Check environment first
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if api_key:
        return api_key

    # 2) Fallback: try to read .env manually (even if python-dotenv is not installed)
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                name, value = line.split("=", 1)
                if name.strip() == "GOOGLE_MAPS_API_KEY":
                    return value.strip().strip("\"'")  # remove possible quotes

    raise RuntimeError(
        "Google API key not found. Please set GOOGLE_MAPS_API_KEY in environment or .env"
    )


def geocode_address(
    address: str, api_key: str
) -> Tuple[Optional[float], Optional[float], Optional[str], Optional[str]]:
    """
    Call Google Geocoding API for a single address.

    Returns:
        (lat, lon, normalized_address, region)
    """
    if not address or not isinstance(address, str):
        return None, None, None, None

    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": address,
        "key": api_key,
        "language": "ru",  # more natural normalized addresses for RU
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
    except Exception:
        return None, None, None, None

    data = response.json()
    if data.get("status") != "OK" or not data.get("results"):
        return None, None, None, None

    result = data["results"][0]

    # Lat / Lon
    location = result.get("geometry", {}).get("location", {})
    lat = location.get("lat")
    lng = location.get("lng")

    # Normalized address
    formatted_address = result.get("formatted_address")

    # Region (administrative_area_level_1 or similar)
    region = None
    for comp in result.get("address_components", []):
        types = comp.get("types", [])
        if "administrative_area_level_1" in types or "administrative_area_level_2" in types:
            region = comp.get("long_name")
            break

    return lat, lng, formatted_address, region


def main():
    # Settings
    source_path = os.path.join("source", "robots.xlsx")
    output_path = os.path.join("export", "robots_geocoded.csv")
    address_column = "Адрес СМ"

    api_key = load_api_key()

    # Read Excel
    df = pd.read_excel(source_path)
    if address_column not in df.columns:
        raise KeyError(
            f"Column '{address_column}' not found in {source_path}. "
            f"Available columns: {list(df.columns)}"
        )

    # Prepare new columns
    df["GEOCODE_LAT"] = None
    df["GEOCODE_LON"] = None
    df["GEOCODE_ADDRESS"] = None
    df["GEOCODE_REGION"] = None

    # Only test on first 3 addresses as requested
    max_rows = 3
    rows_to_process = min(len(df), max_rows)

    for idx in range(rows_to_process):
        address = df.loc[idx, address_column]
        lat, lon, norm_addr, region = geocode_address(address, api_key)
        df.loc[idx, "GEOCODE_LAT"] = lat
        df.loc[idx, "GEOCODE_LON"] = lon
        df.loc[idx, "GEOCODE_ADDRESS"] = norm_addr
        df.loc[idx, "GEOCODE_REGION"] = region

        # Be polite to the API: small delay
        time.sleep(0.2)

    # Save as CSV copy with new columns
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)

    print(f"Done. Geocoded first {rows_to_process} rows and saved to {output_path}")


if __name__ == "__main__":
    main()


