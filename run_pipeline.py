import sys
import os
import time

# --- STEP 1: MODULE RESOLUTION ---
# This ensures 'src' is findable regardless of how you run the script
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

# --- STEP 2: IMPORT YOUR CLASSES/FUNCTIONS ---
from src.ingestion.ingest_climate import ingest_raw_climate
from src.ingestion.ingest_realtor import ingest_realtor_listings
from src.processing.spark_processor import process_bronze_to_silver
from src.models.nlp_agent import enrich_with_nlp

# --- INFRASTRUCTURE PATCH START (Critical for Windows 11 / Java 17) ---
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = (
        os.environ["JAVA_HOME"] + r"\bin;" +
        os.environ["HADOOP_HOME"] + r"\bin;" +
        os.environ["PATH"]
)

# --- INFRASTRUCTURE PATCH END ---


def run_full_pipeline():
    print("🚀 [STARTING] Canadian Real Estate & Climate Risk Pipeline")
    start_time = time.time()

    try:
        # --- STAGE 1: BRONZE (INGESTION) ---
        print("\n📦 STAGE 1: Ingesting Bronze Data...")

        # We run both ingestions. In a senior-level setup, we use 'all()'
        # to ensure both scripts successfully saved their JSON files.
        climate_status = ingest_raw_climate()
        realtor_status = ingest_realtor_listings()

        if not (climate_status and realtor_status):
            print("⚠️ Ingestion partially failed. Checking logs...")

        # --- STAGE 2: SILVER (SPARK PROCESSING) ---
        print("\n⚙️ STAGE 2: Transforming to Silver (PySpark)...")
        # This function already handles your Java 17 and Hadoop patches
        silver_success = process_bronze_to_silver()

        if not silver_success:
            print("❌ Pipeline Halted: Spark Transformation failed.")
            return

        # --- STAGE 3: GOLD (AI ENRICHMENT) ---
        print("\n✨ STAGE 4: Enriching to Gold (NLP Agent)...")
        # This triggers Llama 3 for the red-flag keyword extraction
        enrich_with_nlp()

        # --- FINAL SUMMARY ---
        duration = (time.time() - start_time) / 60
        print(f"\n🏆 [SUCCESS] Full Medallion Pipeline completed in {duration:.2f} minutes.")

    except Exception as e:
        print(f"\n💥 [CRITICAL ERROR] Pipeline crashed: {e}")


if __name__ == "__main__":
    run_full_pipeline()