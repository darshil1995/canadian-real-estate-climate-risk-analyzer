import time
import random
import json
from datetime import datetime
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Import our centralized constants
from src.common.config import REALTOR_BRONZE, SCRAPE_DELAY_RANGE


def ingest_realtor_listings(city="Cambridge", province="Ontario"):
    """
    Ethically scrapes Realtor.ca using undetected-chromedriver.
    Targets Chrome Version 146 specifically to match your environment.
    """
    print(f"--- Starting Stealth Ingestion: {city}, {province} ---")

    options = uc.ChromeOptions()
    # options.add_argument('--headless') # Keep visible to solve Captchas

    try:
        # HARDCODE version_main to 146 to fix your SessionNotCreatedException
        driver = uc.Chrome(options=options, version_main=146)

        # Build search URL
        search_url = f"https://www.realtor.ca/on/{city.lower()}/real-estate"
        driver.get(search_url)

        # --- HUMAN INTERVENTION STEP ---
        print("[ACTION REQUIRED] Please solve any CAPTCHA in the Chrome window.")
        print("[INFO] Waiting 15 seconds for page load...")
        time.sleep(15)

        # Wait for the main listing container to appear
        wait = WebDriverWait(driver, 30)
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "cardCon")))

        # Scroll down slightly to trigger lazy-loading of cards
        driver.execute_script("window.scrollTo(0, 500);")
        time.sleep(2)

        listing_elements = driver.find_elements(By.CLASS_NAME, "cardCon")
        all_listings = []

        print(f"[INFO] Found {len(listing_elements)} cards. Starting extraction...")

        for element in listing_elements[:10]:  # Safety limit for portfolio demo
            try:
                # Extracting data using 2026 CSS Classes
                price_raw = element.find_element(By.CLASS_NAME, "listingCardPrice").text
                address = element.find_element(By.CLASS_NAME, "listingCardAddress").text

                # Capture the full card text for our NLP 'Red Flag' analysis later
                full_summary = element.text.replace("\n", " ")

                # Basic cleaning for the JSON layer
                listing = {
                    "price_str": price_raw.strip(),
                    "address": address.strip(),
                    "fsa": address.split()[-3] if len(address.split()) > 3 else "Unknown",
                    "description_text": full_summary,
                    "ingested_at": datetime.now().isoformat()
                }
                all_listings.append(listing)

                # Ethical Delay: Don't click/scrape too fast
                time.sleep(random.uniform(1.5, 3.5))

            except Exception as e:
                print(f"[SKIP] Card extraction failed: {e}")
                continue

        # --- SAVE TO BRONZE LAYER ---
        if all_listings:
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M")
            file_path = REALTOR_BRONZE / f"realtor_raw_{timestamp_str}.json"

            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(all_listings, f, indent=4)

            print(f"\n[SUCCESS] Saved {len(all_listings)} listings to:")
            print(f" >> {file_path}")
            return str(file_path)
        else:
            print("[ERROR] No listings were captured. Check the browser window.")
            return None

    except Exception as overall_e:
        print(f"[CRITICAL] Scraper crashed: {overall_e}")
        return None
    finally:
        # Close browser safely
        if 'driver' in locals():
            driver.quit()


if __name__ == "__main__":
    ingest_realtor_listings()