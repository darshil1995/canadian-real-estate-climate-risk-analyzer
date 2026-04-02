import pandas as pd
import ollama
from src.common.config import SILVER_DIR, GOLD_DIR
from tqdm import tqdm
import os
tqdm.pandas(desc="AI Analysis Progress")


def analyze_red_flags(description):
    """
    Uses System Role and Few-Shot prompting via Ollama Chat API
    to force strict keyword output.
    """
    if not description or len(description) < 20:
        return "N/A"

    try:

        # Check if we are running in Docker or Local
        # (Docker usually sets the 'IS_DOCKER' env var if you add it)
        OLLAMA_URL = "http://host.docker.internal:11434" if os.getenv("IS_DOCKER") else "http://localhost:11434"
        client = ollama.Client(host=OLLAMA_URL)

        response = ollama.chat(
            model='llama3',
            messages=[
                {
                    'role': 'system',
                    'content': (
                        "You are a cold, efficient data extraction engine. "
                        "Your ONLY job is to identify real estate risk keywords. "
                        "Rules: 1. Output ONLY keywords separated by commas. "
                        "2. If no risks exist, output 'Clean'. "
                        "3. NEVER use sentences, introductions, or explanations. "
                        "4. Example: 'Mold, Basement Leaks'. Example: 'Clean'."
                    )
                },
                {
                    'role': 'user',
                    'content': f"Analyze this description: {description}"
                }
            ],
            options={
                "temperature": 0.0,
                "stop": ["\n", "Based on", "Here is"]
            }
        )

        # Extract content and perform a final 'strip' of any remaining fluff
        result = response['message']['content'].strip()

        # Final safety: If the model still tries to explain, we take the first line
        # and remove common prefix phrases manually.
        clean_result = result.split('\n')[0]
        prefixes = ["Output:", "Keywords:", "Risk Keywords:", "Red Flags:"]
        for prefix in prefixes:
            clean_result = clean_result.replace(prefix, "")

        return clean_result.strip()

    except Exception as e:
        return f"Error: {str(e)}"

def enrich_with_nlp():
    print("\n--- Starting NLP Enrichment (Gold Layer) ---")

    # Path to the Silver Parquet folder we just created
    silver_path = SILVER_DIR / "unified_listings_v1"

    try:
        # 1. Read Silver data into Pandas (Easier for row-by-row LLM calls)
        df = pd.read_parquet(silver_path)

        if df.empty: return

        print(f"Processing {len(df)} listings through Llama 3...")

        # 2. Run the analysis
        df['climate_red_flags'] = df['description_text'].progress_apply(analyze_red_flags)

        # 3. Create Gold Directory and Save
        GOLD_DIR.mkdir(parents=True, exist_ok=True)
        output_path = GOLD_DIR / "enriched_listings_v1.parquet"
        df.to_parquet(output_path)

        print(f"[SUCCESS] Gold Layer Created: {output_path}")
        print("\n--- Sample Results ---")
        print(df[['address', 'climate_red_flags']].head())

    except Exception as e:
        print(f"[ERROR] NLP enrichment failed: {e}")


if __name__ == "__main__":
    enrich_with_nlp()