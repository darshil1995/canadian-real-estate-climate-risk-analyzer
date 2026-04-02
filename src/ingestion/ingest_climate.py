import requests
import json
from datetime import datetime
from src.common.config import CLIMATE_API_BASE_URL, DEFAULT_STATION_ID, CLIMATE_LIMIT, CLIMATE_BRONZE


def ingest_raw_climate(station_id=DEFAULT_STATION_ID):
    """
    Fetches raw daily climate data from Environment Canada.
    Saves to the Bronze directory as a raw JSON snapshot.
    """
    print(f"--- Starting Ingestion: Station {station_id} ---")

    params = {
        "STN_ID": station_id,
        "limit": CLIMATE_LIMIT,
        "f": "json"
    }

    try:
        response = requests.get(CLIMATE_API_BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        # Create a unique filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        file_path = CLIMATE_BRONZE / f"climate_raw_{station_id}_{timestamp}.json"

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

        print(f"[SUCCESS] Ingested {len(data.get('features', []))} records.")
        print(f"[INFO] Saved to: {file_path}")
        return str(file_path)

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] API Request failed: {e}")
        return None


if __name__ == "__main__":
    ingest_raw_climate()