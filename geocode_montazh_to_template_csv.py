"""
geocode_montazh_to_template_csv.py — геокодирование адресов из Excel в CSV-шаблон.

Читает xlsx-файл "Монтаж Роботов уборщиков.xlsx", геокодирует адреса через Google
Geocoding API и выгружает в export/montazh_geocoded_template_ru.csv в формате шаблона
(Широта;Долгота;Описание;Подпись;Номер метки).

Особенности:
- Пропуск пустых строк (нет адреса в столбце «Адрес»).
- Кеш geocode_cache.json: повторные адреса и координаты из xlsx не вызывают API.
- Если в xlsx есть столбцы «Широта» и «Долгота» — используются их значения.

Настройка: GOOGLE_MAPS_API_KEY в .env или переменной окружения.
Запуск: python geocode_montazh_to_template_csv.py
"""

import json
import os
import time
from typing import Any, Dict, Iterable, Optional, Tuple

import pandas as pd
import requests

GEOCODE_CACHE_FILENAME = "geocode_cache.json"
SOURCE_XLSX_FILENAME = "_Монтаж Роботов уборщиков.xlsx"
OUTPUT_CSV_DIR = "export"
OUTPUT_CSV_FILENAME = "montazh_geocoded_template_ru.csv"


def load_api_key() -> str:
    """
    Load Google Geocoding API key from .env or environment.

    Expects a variable named GOOGLE_MAPS_API_KEY either in:
    - OS environment, or
    - .env file in the project root (format: GOOGLE_MAPS_API_KEY=...).
    """
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if api_key:
        return api_key

    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                name, value = line.split("=", 1)
                if name.strip() == "GOOGLE_MAPS_API_KEY":
                    return value.strip().strip("\"'")

    raise RuntimeError(
        "Google API key not found. Please set GOOGLE_MAPS_API_KEY in environment or .env"
    )


def geocode_address(
    address: str, api_key: str
) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """
    Call Google Geocoding API for a single address.

    Returns:
        (lat, lon, formatted_address)
    """
    if not address or not isinstance(address, str):
        return None, None, None

    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"address": address, "key": api_key, "language": "ru"}

    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
    except Exception:
        return None, None, None

    data = response.json()
    if data.get("status") != "OK" or not data.get("results"):
        return None, None, None

    result = data["results"][0]
    location = result.get("geometry", {}).get("location", {})
    lat = location.get("lat")
    lng = location.get("lng")
    formatted_address = result.get("formatted_address")
    return lat, lng, formatted_address


def _is_number(value: Any) -> bool:
    # bool is a subclass of int, exclude it
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _csv_escape(text: str) -> str:
    return text.replace('"', '""')


def _csv_field(value: Any) -> str:
    """
    Produce a CSV field formatted to match template_CSV_ru.csv style:
    - numbers are unquoted (e.g. 55.7)
    - strings are always quoted with double quotes
    - missing values are blank (unquoted)
    """
    if value is None or pd.isna(value):
        return ""

    if _is_number(value):
        return str(value)

    s = str(value).strip()
    if s == "":
        return ""
    return f'"{_csv_escape(s)}"'


def _normalize_signature(value: Any) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    # Always treat signature as text (should be quoted in output)
    return str(value).strip()


def _normalize_label(value: Any) -> Optional[int]:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        # If it's e.g. 12.0 from Excel, convert to int
        if value.is_integer():
            return int(value)
        return None
    if isinstance(value, str):
        s = value.strip()
        if s == "":
            return None
        try:
            return int(s)
        except ValueError:
            return None
    return None


def write_template_csv(rows: list[list[Any]], output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Write manually to guarantee quoting rules exactly (as in template_CSV_ru.csv)
    header = ["Широта", "Долгота", "Описание", "Подпись", "Номер метки"]
    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        f.write(";".join(_csv_field(h) for h in header) + "\n")
        for row in rows:
            f.write(";".join(_csv_field(v) for v in row) + "\n")


def _normalize_header(name: Any) -> str:
    """
    Normalize header names to improve matching across slightly different Excel exports.
    """
    if name is None or pd.isna(name):
        return ""
    s = str(name).strip().lower()
    # collapse whitespace
    s = " ".join(s.split())
    # common punctuation/format variants
    s = s.replace("\u00a0", " ")  # NBSP
    return s


def _load_geocode_cache(cache_path: str) -> Dict[str, Dict[str, Any]]:
    """Load geocoding cache from JSON file."""
    if not os.path.exists(cache_path):
        return {}
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        return {}


def _save_geocode_cache(cache: Dict[str, Dict[str, Any]], cache_path: str) -> None:
    """Save geocoding cache to JSON file."""
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def _extract_lat_lon_from_row(
    df: pd.DataFrame, idx: int, lat_col: str, lon_col: str
) -> Tuple[Optional[float], Optional[float]]:
    """Extract lat/lon from a row if both are valid numbers."""
    lat_val = df.loc[idx, lat_col]
    lon_val = df.loc[idx, lon_col]
    if pd.isna(lat_val) or pd.isna(lon_val):
        return None, None
    try:
        lat = float(lat_val)
        lon = float(lon_val)
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            return lat, lon
    except (TypeError, ValueError):
        pass
    return None, None


def _pick_column_optional(
    df: pd.DataFrame, candidates: Iterable[str]
) -> Optional[str]:
    """Pick the first existing column from candidates, or None if none match."""
    cols = list(df.columns)
    for c in candidates:
        if c in df.columns:
            return c
    norm_to_actual: Dict[str, str] = {}
    for col in cols:
        norm_to_actual[_normalize_header(col)] = col
    for c in candidates:
        norm = _normalize_header(c)
        if norm in norm_to_actual and norm_to_actual[norm] in df.columns:
            return norm_to_actual[norm]
    return None


def _pick_column(df: pd.DataFrame, candidates: Iterable[str]) -> str:
    """
    Pick the first existing column from candidates, using normalized matching as fallback.
    """
    cols = list(df.columns)
    # 1) exact match
    for c in candidates:
        if c in df.columns:
            return c

    # 2) normalized match
    norm_to_actual: Dict[str, str] = {}
    for col in cols:
        norm_to_actual[_normalize_header(col)] = col
    for c in candidates:
        norm = _normalize_header(c)
        if norm in norm_to_actual and norm_to_actual[norm] in df.columns:
            return norm_to_actual[norm]

    raise KeyError(
        f"None of the candidate columns exist: {list(candidates)}. Available: {cols}"
    )


def main() -> None:
    source_path = os.path.join(os.path.dirname(__file__), SOURCE_XLSX_FILENAME)
    output_path = os.path.join(
        os.path.dirname(__file__), OUTPUT_CSV_DIR, OUTPUT_CSV_FILENAME
    )

    # Accept common header variants from different Excel templates/exports
    address_column = "Адрес"
    address_candidates = [address_column, "Адрес объекта", "Адрес установки", "address"]

    sap_column = "SAP"
    sap_candidates = [sap_column, "Sap", "SAP ID", "САП", "сап", "sap"]

    label_column = "№"
    label_candidates = [label_column, "№ п/п", "№п/п", "№ п\\п", "Номер", "No", "№п.п"]

    api_key = load_api_key()

    # Try reading with different parameters to handle merged cells and empty values better
    # Check available sheets and use the correct one (prefer second sheet if it exists)
    try:
        excel_file = pd.ExcelFile(source_path)
        sheet_names = excel_file.sheet_names
        print(f"Available sheets: {sheet_names}", flush=True)
        
        # Use second sheet (index 1) if available, as it contains the actual data
        # First sheet appears to be a summary/template
        target_sheet_idx = 1 if len(sheet_names) > 1 else 0
        print(f"Using sheet index {target_sheet_idx}: '{sheet_names[target_sheet_idx]}'", flush=True)
        
        # Read with keep_default_na=False to preserve empty strings
        df = pd.read_excel(source_path, sheet_name=target_sheet_idx, keep_default_na=False, na_values=[])
    except Exception as e:
        print(f"Warning: {e}, falling back to standard read", flush=True)
        # Fallback: standard read
        df = pd.read_excel(source_path)
    
    # Resolve columns against actual sheet headers
    address_column = _pick_column(df, address_candidates)
    sap_column = _pick_column(df, sap_candidates)
    label_column = _pick_column(df, label_candidates)

    lat_candidates = ["Широта", "Latitude", "lat"]
    lon_candidates = ["Долгота", "Longitude", "lon"]
    lat_col = _pick_column_optional(df, lat_candidates)
    lon_col = _pick_column_optional(df, lon_candidates)
    if lat_col and lon_col and lat_col == lon_col:
        lon_col = None

    cache_path = os.path.join(os.path.dirname(__file__), GEOCODE_CACHE_FILENAME)
    cache_dict: Dict[str, Dict[str, Any]] = _load_geocode_cache(cache_path)

    def _get_from_cache(addr: str) -> Tuple[Optional[float], Optional[float], Optional[str]]:
        entry = cache_dict.get(addr)
        if not entry:
            return None, None, None
        return (
            entry.get("lat"),
            entry.get("lon"),
            entry.get("formatted_address"),
        )

    def _put_to_cache(
        addr: str,
        lat: Optional[float],
        lon: Optional[float],
        formatted: Optional[str],
    ) -> None:
        cache_dict[addr] = {
            "lat": lat,
            "lon": lon,
            "formatted_address": formatted,
        }

    out_rows: list[list[Any]] = []
    total = len(df)
    processed = 0
    skipped = 0

    for i in range(total):
        raw_address = df.loc[i, address_column]
        if pd.isna(raw_address) or raw_address is None:
            address = ""
        elif isinstance(raw_address, str):
            address = raw_address.strip()
        else:
            address = str(raw_address).strip() if raw_address else ""

        # 1. Skip empty address rows
        if address == "":
            skipped += 1
            continue

        lat: Optional[float]
        lon: Optional[float]
        geocode_address_norm: Optional[str]

        # 2. If xlsx has lat/lon columns and row has valid values, use them
        if lat_col and lon_col:
            xlsx_lat, xlsx_lon = _extract_lat_lon_from_row(df, i, lat_col, lon_col)
            if xlsx_lat is not None and xlsx_lon is not None:
                lat, lon = xlsx_lat, xlsx_lon
                _, _, cached_formatted = _get_from_cache(address)
                geocode_address_norm = cached_formatted if cached_formatted else address
                _put_to_cache(address, lat, lon, geocode_address_norm)
            else:
                entry = _get_from_cache(address)
                if entry[0] is not None:
                    lat, lon, geocode_address_norm = entry
                else:
                    lat, lon, geocode_address_norm = geocode_address(address, api_key)
                    _put_to_cache(address, lat, lon, geocode_address_norm)
                    time.sleep(0.2)
        else:
            entry = _get_from_cache(address)
            if entry[0] is not None:
                lat, lon, geocode_address_norm = entry
            else:
                lat, lon, geocode_address_norm = geocode_address(address, api_key)
                _put_to_cache(address, lat, lon, geocode_address_norm)
                time.sleep(0.2)

        signature = _normalize_signature(df.loc[i, sap_column])
        label = _normalize_label(df.loc[i, label_column])

        out_rows.append([lat, lon, geocode_address_norm, signature, label])
        processed += 1

        if processed % 50 == 0 or i + 1 == total:
            print(f"Processed {processed} rows, skipped {skipped} empty", flush=True)

    _save_geocode_cache(cache_dict, cache_path)
    write_template_csv(out_rows, output_path)
    print(f"Done. Processed {processed} rows, skipped {skipped} empty. Saved: {output_path}", flush=True)
    print(f"Geocode cache: {len(cache_dict)} entries -> {cache_path}", flush=True)


if __name__ == "__main__":
    main()

