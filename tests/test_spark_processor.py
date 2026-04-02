import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

def test_realtor_price_cleaning(spark):
    # 1. MOCK DATA: Simulate what your scraper produces
    data = [("$500,000", "123 Main St"), ("450000", "456 Lake Rd"), (None, "789 Park")]
    columns = ["price_str", "address"]
    df = spark.createDataFrame(data, columns)

    # 2. APPLY YOUR LOGIC (Extracted from spark_processor.py)
    cleaned_df = df.withColumn(
        "price",
        F.regexp_replace(F.col("price_str"), r"[\$,]", "").cast(IntegerType())
    )

    # 3. ASSERTIONS
    results = cleaned_df.collect()
    assert results[0]["price"] == 500000
    assert results[1]["price"] == 450000
    assert results[2]["price"] is None  # Check null handling