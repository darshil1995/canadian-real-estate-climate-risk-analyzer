import pandas as pd
import pytest

from src.common.config import GOLD_DIR


def test_gold_parquet_schema():
    gold_file = GOLD_DIR / "enriched_listings_v1.parquet"

    if not gold_file.exists():
        pytest.skip("Gold file doesn't exist yet. Run the pipeline first.")

    df = pd.read_parquet(gold_file)

    # 1. Check required columns exist
    required_cols = ['address', 'price', 'climate_red_flags', 'avg_max_temp']
    for col in required_cols:
        assert col in df.columns

    # 2. Check for empty AI results
    # We want to ensure the AI didn't just return errors for every row
    error_count = df['climate_red_flags'].str.contains("Error", na=False).sum()
    assert error_count < (len(df) * 0.5), "More than 50% of AI calls failed!"