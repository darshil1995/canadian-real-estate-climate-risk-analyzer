# 🏡 Canadian Real Estate Climate Risk Analyzer
> **A Modern Data Stack for Property Risk Assessment** — End-to-end pipeline from raw ingestion to AI-driven environmental insights.

---

## 📌 Overview

This project is a full-stack data engineering solution that ingests real estate listings and historical climate data to identify environmental **"Red Flags"** using **PySpark** and **Llama 3**. It transforms raw, unstructured data into a structured **Gold layer** suitable for investment analysis and risk scoring.

**Key Capabilities:**
- Scrapes live property listings from Realtor.ca via `undetected-chromedriver`
- Fetches historical climate data from the Environment Canada API
- Joins and enriches datasets using PySpark at scale
- Runs LLM-powered NLP to extract climate risk keywords from listing descriptions
- Serves insights through an interactive Streamlit dashboard

---

## 🏗️ Architecture: Medallion Pattern

The pipeline follows the **Medallion Architecture**, ensuring data quality and full lineage at every stage:

```
Raw Sources
    │
    ▼
┌─────────────────────────────────────────────────────────────────┐
│  🥉 BRONZE  │  Raw JSON — Realtor.ca listings + Climate API     │
├─────────────────────────────────────────────────────────────────┤
│  🥈 SILVER  │  PySpark — Clean, join, FSA-key, Parquet output   │
├─────────────────────────────────────────────────────────────────┤
│  🥇 GOLD    │  Llama 3 NLP — Red Flag extraction per listing    │
├─────────────────────────────────────────────────────────────────┤
│  📊 SERVE   │  Streamlit Dashboard — Explore & run pipeline     │
└─────────────────────────────────────────────────────────────────┘
```

| Layer | Tool | Output |
|---|---|---|
| Bronze | `undetected-chromedriver`, Requests | Raw JSON files |
| Silver | PySpark | Cleaned `.parquet` datasets |
| Gold | Ollama + Llama 3 | Risk-annotated Parquet |
| Serving | Streamlit | Interactive web dashboard |

---

## 📁 Project Structure

```
realtor-climate-app/
├── run_pipeline.py          # 🔁 Orchestrator — runs all layers sequentially
├── Dockerfile               # 🐳 Python 3.11 + Java 21 containerized environment
├── requirements.txt
│
├── src/
│   ├── ingestion/           # 🥉 Bronze Layer
│   │   ├── ingest_realtor.py      # Scrapes listings via undetected-chromedriver
│   │   └── ingest_climate.py      # Fetches Environment Canada API data
│   │
│   ├── processing/          # 🥈 Silver Layer
│   │   └── spark_processor.py     # PySpark transforms, joins, Parquet writes
│   │
│   ├── models/              # 🥇 Gold Layer
│   │   └── nlp_agent.py           # Few-shot Llama 3 Red Flag extraction
│   │
│   ├── ui/                  # 📊 Serving Layer
│   │   └── dashboard.py           # Streamlit dashboard
│   │
│   └── common/              # 🔧 Utilities
│       └── spark_config.py        # Java/Hadoop cross-platform patches
│
├── data/
│   ├── bronze/              # Raw JSON ingestion outputs
│   ├── silver/              # Cleaned Parquet files
│   └── gold/                # AI-enriched Parquet files
│
└── tests/
    ├── test_spark.py         # Unit tests — transforms & regex
    ├── test_llm.py           # Integration tests — LLM connectivity
    └── test_schema.py        # Data integrity — Gold schema validation
```

---

## ⚙️ Core Implementation Details

### 🥈 Silver Layer — Spark Transformation (`spark_processor.py`)

Handles "dirty" ingested data by:

- **Regex cleaning** — strips currency symbols/commas from price strings into typed integers
- **FSA standardization** — normalizes forward sortation area codes to serve as join keys
- **Dataset join** — inner join between real estate listings and climate metrics on FSA
- **Aggregation** — computes average temperature and precipitation risk metrics per region
- **Output** — writes structured `.parquet` files for downstream consumption

### 🥇 Gold Layer — AI Risk Agent (`nlp_agent.py`)

Implements a **"Cold Extraction Engine"** prompt strategy:

- **Model:** Llama 3 via Ollama (`host.docker.internal`)
- **Temperature:** `0.0` — enforces deterministic, keyword-only outputs
- **Technique:** Few-shot prompting to extract specific risk terms (e.g., `"Mold, Basement Leaks, Flood Zone"`)
- **Input:** Property description text from Silver layer
- **Output:** Structured risk label appended to Gold Parquet records

### 🐳 Containerization (`Dockerfile`)

The project is fully containerized to manage complex runtime dependencies:

- **Python 3.11** — application runtime
- **Java 21** — required by the PySpark engine
- **Hybrid Networking** — container communicates with a host-based Ollama instance via `host.docker.internal` for GPU-accelerated inference without bundling model weights into the image

---

## 🚀 Getting Started

### Prerequisites

| Dependency | Purpose |
|---|---|
| [Docker Desktop](https://www.docker.com/products/docker-desktop/) | Container runtime |
| [Ollama](https://ollama.com/) | Local LLM inference |
| `llama3` model | Red Flag NLP extraction |

Pull the model before running:
```bash
ollama pull llama3
```

### Installation & Execution

**1. Clone the repository**
```bash
git clone https://github.com/your-username/realtor-climate-app.git
cd realtor-climate-app
```

**2. Build the Docker image**
```powershell
docker build -t realtor-climate-app .
```

**3. Run the container**
```powershell
docker run -d -p 8501:8501 \
  --name climate_app \
  --add-host=host.docker.internal:host-gateway \
  -v ${PWD}/data:/app/data \
  realtor-climate-app
```

**4. Open the dashboard**

Navigate to: [http://localhost:8501](http://localhost:8501)

Use the **"Run Pipeline"** button in the dashboard to trigger the full Bronze → Silver → Gold execution.

---

## 🧪 Testing

The `pytest` suite in `/tests` covers three categories:

| Test Type | File | What It Validates |
|---|---|---|
| Unit | `test_spark.py` | Spark transformation logic, regex cleaning functions |
| Integration | `test_llm.py` | App ↔ Local LLM connectivity and response format |
| Data Integrity | `test_schema.py` | Schema validation of final Gold Parquet output |

Run all tests:
```bash
pytest tests/ -v
```

---

## 🗺️ Pipeline Flow

```
[Realtor.ca]          [Environment Canada API]
      │                         │
      ▼                         ▼
 ingest_realtor.py       ingest_climate.py
      │                         │
      └──────────┬──────────────┘
                 ▼
         spark_processor.py
         (clean → join → parquet)
                 │
                 ▼
           nlp_agent.py
         (Llama 3 Red Flags)
                 │
                 ▼
          dashboard.py
        (Streamlit UI)
```

---

## 👤 Author

**Darshil**
Focus: AI/ML Engineering & Data Science

---

*Built with PySpark · Llama 3 · Streamlit · Docker · Ollama*