# azure-etl-framework

A modular, production-grade Python ETL framework for Azure data pipelines — supporting the **Medallion Architecture** (Bronze → Silver → Gold).

[![CI](https://github.com/vhemraj847/azure-etl-framework/actions/workflows/ci.yml/badge.svg)](https://github.com/vhemraj847/azure-etl-framework/actions)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![Azure](https://img.shields.io/badge/Azure-Data%20Engineering-0078D4)

---

## Features

- **Plug-and-play architecture** — swap extractors, transformers, and loaders independently
- **Medallion-ready** — built-in audit columns, layer tagging, and Bronze→Silver→Gold patterns
- **Production logging** — structured logs at every pipeline stage
- **Fully tested** — pytest suite with 80%+ coverage enforced via CI
- **Azure-native** — ADLS Gen2, Azure SQL, and ADF-compatible outputs

---

## Quick Start

```bash
git clone https://github.com/vhemraj847/azure-etl-framework.git
cd azure-etl-framework
pip install -r requirements.txt
python examples/run_local_pipeline.py
```

**Expected output:**
```
============================================================
ConsoleLoader — 3 records
============================================================
{
  "order_id": "1001",
  "customer_id": "C001",
  "amount": 15000.0,
  "revenue_band": "high",
  "_ingested_at": "2024-...",
  "_layer": "silver"
}
...
```

---

## Architecture

```
Source System
    │
    ▼
BaseExtractor          ← ADLSExtractor / SQLExtractor / custom
    │
    ▼
BaseTransformer        ← MedallionTransformer / SalesTransformer / custom
    │  (Bronze → Silver → Gold)
    ▼
BaseLoader             ← ADLSLoader / SQLLoader / ConsoleLoader / custom
    │
    ▼
Target (ADLS / SQL / Delta Lake)
```

---

## Project Structure

```
azure-etl-framework/
├── etl/
│   ├── __init__.py
│   ├── pipeline.py       # Core Pipeline, Base classes
│   ├── extractors.py     # ADLSExtractor, SQLExtractor
│   ├── transformers.py   # MedallionTransformer, SalesTransformer
│   └── loaders.py        # ADLSLoader, SQLLoader, ConsoleLoader
├── examples/
│   └── run_local_pipeline.py
├── tests/
│   └── test_pipeline.py
├── .github/workflows/ci.yml
├── requirements.txt
└── README.md
```

---

## Running Tests

```bash
pytest tests/ -v --cov=etl
```

---

## Built With

- Python 3.11
- azure-storage-file-datalake
- azure-identity
- pyodbc
- pytest

---

*Part of Hemraj Verma's Azure Data Engineering portfolio.*
