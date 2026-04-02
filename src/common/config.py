import os
from pathlib import Path

# --- Directory Structure ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# Sub-folders for organization
REALTOR_BRONZE = BRONZE_DIR / "realtor"
CLIMATE_BRONZE = BRONZE_DIR / "climate"

for folder in [REALTOR_BRONZE, CLIMATE_BRONZE, SILVER_DIR, GOLD_DIR]:
    folder.mkdir(parents=True, exist_ok=True)

# --- Ethical Scraping Constants ---
# Using a real-looking browser header
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}
# Respectful delay between requests (seconds)
SCRAPE_DELAY_RANGE = (3, 7)

# --- Environment Canada API ---
CLIMATE_API_BASE_URL = "https://api.weather.gc.ca/collections/climate-daily/items"
DEFAULT_STATION_ID = "51459"
CLIMATE_LIMIT = 1000

# --- MLflow ---
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001")
EXPERIMENT_NAME = "RealEstate_Climate_Risk"

# --- Feature Engineering ---
# We will join on the first 3 characters of the Postal Code (Forward Sortation Area)
JOIN_KEY = "fsa"