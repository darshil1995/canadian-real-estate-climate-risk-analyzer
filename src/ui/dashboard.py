import streamlit as st
import pandas as pd
import subprocess
import sys
import time
import os
from pathlib import Path

# --- BOOTSTRAP: Ensure 'src' is findable ---
# This mirrors the PYTHONPATH = "." behavior programmatically
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.common.config import GOLD_DIR, SILVER_DIR

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="🍁 Canadian Real Estate Climate Risk",
    page_icon="🏡",
    layout="wide"
)

# --- STYLING ---
st.markdown("""
    <style>
    .main { background-color: #f5f7f9; }
    .stMetric { background-color: #ffffff; padding: 15px; border-radius: 10px; box-shadow: 2px 2px 5px rgba(0,0,0,0.05); }
    </style>
    """, unsafe_allow_html=True)


# --- DATA LOADING ---
@st.cache_data
def load_gold_data():
    gold_file = GOLD_DIR / "enriched_listings_v1.parquet"
    if not gold_file.exists():
        return None
    # engine='pyarrow' is recommended for performance
    return pd.read_parquet(gold_file)


# --- SIDEBAR: PIPELINE CONTROL ---
with st.sidebar:
    st.header("⚙️ Pipeline Control Center")
    st.markdown("---")

    # 1. FULL PIPELINE TRIGGER
    if st.button("🔥 Run FULL Medallion Pipeline", use_container_width=True):
        progress_bar = st.progress(0, text="Starting Medallion Architecture...")

        # We call the orchestration script we created
        process = subprocess.Popen([sys.executable, "run_pipeline.py"])

        # Real-time progress monitoring via the status file handshake
        while process.poll() is None:
            if os.path.exists("progress_status.txt"):
                with open("progress_status.txt", "r") as f:
                    content = f.read().strip()
                    if "," in content:
                        current, total = map(int, content.split(","))
                        percent = min(current / total, 1.0)
                        progress_bar.progress(percent, text=f"AI Enrichment: {current}/{total} listings...")
            time.sleep(0.5)

        if process.returncode == 0:
            st.success("Full Pipeline Execution Successful!")
            st.cache_data.clear()  # Clear cache to force reload of new data
            st.rerun()
        else:
            st.error("Pipeline Execution Failed. Check Terminal.")

    st.markdown("---")
    st.info("The dashboard reflects data from the **Gold Layer** (Enriched Parquet).")

# --- MAIN DASHBOARD VIEW ---
st.title("🏡 Canadian Real Estate Climate Risk Analyzer")
st.markdown("Enriched with **PySpark** and **Llama 3** (Local Inference)")

df = load_gold_data()

if df is not None:
    # 1. METRICS ROW
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Listings", len(df))
    m2.metric("Avg Listing Price", f"${df['price'].mean():,.0f}")

    # Logic: Count any row where red_flags is NOT 'Clean', 'None', or 'N/A'
    risky_df = df[~df['climate_red_flags'].str.contains("Clean|None|N/A", case=False, na=False)]
    m3.metric("High Risk Properties", len(risky_df), delta="AI Flagged", delta_color="inverse")

    avg_temp = df['avg_max_temp'].iloc[0] if 'avg_max_temp' in df.columns else 0
    m4.metric("Region Max Temp", f"{avg_temp:.1f}°C")

    st.divider()

    # 2. DATASET EXPLORER
    c1, c2 = st.columns([2, 1])

    with c1:
        st.subheader("📋 Property Risk Explorer")
        search = st.text_input("🔍 Search Red Flags (e.g., 'Basement', 'As-is', 'Mold')")

        display_df = df.copy()
        if search:
            display_df = display_df[display_df['climate_red_flags'].str.contains(search, case=False, na=False)]

        st.dataframe(
            display_df[['address', 'price', 'climate_red_flags', 'avg_max_temp']],
            use_container_width=True,
            hide_index=True
        )

    with c2:
        st.subheader("📊 Investment Overview")
        # Visualizing the distribution of prices across Cambridge
        st.bar_chart(df.set_index('address')['price'])

        st.caption("Price distribution for currently ingested Cambridge listings.")

else:
    st.warning("⚠️ No Gold data found. Please run the **FULL Medallion Pipeline** from the sidebar.")