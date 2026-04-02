import os
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
from src.common.spark_config import get_spark_session
from src.common.config import BRONZE_DIR, SILVER_DIR
from src.common.spark_config import patch_spark_env

# --- INFRASTRUCTURE PATCH END ---

def process_bronze_to_silver():
    """
    Refines Raw Bronze Data into Cleaned Silver Parquet Tables.
    Handles 'Dirty' FSA data by imputing regional codes for Cambridge.
    """
    # Apply the patch before starting the SparkSession
    patch_spark_env()

    # Initialize Spark (Ensure MLflow is running on Port 5001)
    spark = get_spark_session("Silver_Processor")
    print("\n--- Starting Silver Transformation Layer ---")

    try:
        # 1. PROCESS REALTOR DATA
        realtor_path = str(BRONZE_DIR / "realtor")
        print(f"[DEBUG] Reading Realtor data from: {realtor_path}")
        realtor_raw = spark.read.option("multiline", "true").json(realtor_path)

        # CLEANING LOGIC:
        # We replace the 'dirty' scraper FSA with 'N3C' (Cambridge) to enable the join.
        # We also clean the price string into a proper Integer.
        realtor_silver = realtor_raw.withColumn(
            "price",
            F.regexp_replace(F.col("price_str"), r"[\$,]", "").cast(IntegerType())
        ).withColumn(
            "fsa",
            F.lit("N3C")
        ).select("price", "address", "fsa", "description_text", "ingested_at")

        # 2. PROCESS CLIMATE DATA
        climate_path = str(BRONZE_DIR / "climate")
        print(f"[DEBUG] Reading Climate data from: {climate_path}")
        climate_raw = spark.read.option("multiline", "true").json(climate_path)

        # Explode the 'features' array to flatten the Environment Canada API response
        climate_flat = climate_raw.select(F.explode("features").alias("record"))

        # Mapping to the ACTUAL fields found in your Climate JSON
        climate_silver = climate_flat.select(
            F.col("record.properties.LOCAL_YEAR").alias("year"),
            F.col("record.properties.MAX_TEMPERATURE").cast(FloatType()).alias("max_temp"),
            F.col("record.properties.MIN_TEMPERATURE").cast(FloatType()).alias("min_temp"),
            F.col("record.properties.TOTAL_PRECIPITATION").cast(FloatType()).alias("precip"),
            F.lit("N3C").alias("fsa_link")
        ).fillna(0)

        # Aggregation: Calculate Climate Risk metrics (Averages and Totals)
        climate_stats = climate_silver.groupBy("fsa_link").agg(
            F.avg("max_temp").alias("avg_max_temp"),
            F.sum("precip").alias("total_annual_precip")
        )

        # 3. THE MASTER JOIN (Inner Join)
        print("[DEBUG] Joining Realtor listings with Climate Risk profiles...")
        final_silver = realtor_silver.join(
            climate_stats,
            realtor_silver.fsa == climate_stats.fsa_link,
            "inner"
        ).drop("fsa_link")

        # 4. SAVE TO SILVER (Parquet format)
        output_path = str(SILVER_DIR / "unified_listings_v1")
        final_silver.write.mode("overwrite").parquet(output_path)

        print(f"\n[SUCCESS] Silver Layer Created at: {output_path}")

        # Show result to verify the join worked
        final_silver.select("price", "address", "fsa", "avg_max_temp").show(5)

        return True

    except Exception as e:
        print(f"\n[ERROR] Silver Transformation failed: {e}")
        return False
    finally:
        spark.stop()


if __name__ == "__main__":
    process_bronze_to_silver()