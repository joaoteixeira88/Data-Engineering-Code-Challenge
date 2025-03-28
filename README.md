# PySpark Sales Data Pipeline

[![Python](https://img.shields.io/badge/python-3.9+-informational.svg)](https://www.python.org)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![documentation style: google](https://img.shields.io/badge/%20style-google-3666d6.svg)](https://google.github.io/styleguide/pyguide.html#s3.8-comments-and-docstrings)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)


## 📌 Project Overview
This project processes and analyzes sales transaction data using **PySpark**. It includes:
- **Data Loading**: Reads and normalizes transaction, product, and store data.
- **Data Transformation**: Computes revenue, sales insights, and enriches data.
- **Data Export**: Saves processed data in **Parquet** and **CSV** formats.

---

## 🛠️ Setup & Installation
### 1️⃣ Prerequisites
Ensure you have the following installed:
- Python 3.9+
- Apache Spark
- Poetry (for dependency management)

### 2️⃣ Install Dependencies
Use Poetry to install dependencies:
```bash
poetry shell
poetry lock
poetry install
```
---

## 🚀 Running the Project
### 1️⃣ Load Data
The `LoadData` class reads sales, products, and store data:
```python
from src.data_processing.load_data import LoadData

loader = LoadData()
loader.load_data()
```

### 2️⃣ Transform Data
Use `TransformationData` for sales aggregation and enrichment:
```python
from src.data_processing.transformation_data import TransformationData

transformer = TransformationData(loader.transactions, loader.products, loader.stores)
revenue_df = transformer.calculate_total_revenue()
monthly_sales_df = transformer.calculate_monthly_sales()
enriched_df = transformer.enrich_sales_data()
```

### 3️⃣ Export Data
Processed data is saved in Parquet and CSV:
```python
enriched_df.write.parquet("output/enriched_data", partitionBy=["category", "transaction_date"])
revenue_df.write.csv("output/store_revenue.csv", header=True)
```

---

## ✅ Running Tests
This project uses `pytest` and `chispa` for testing. To run tests:
```bash
pytest tests/
```
Tests cover:
- **Date normalization** ✅
- **Revenue calculations** ✅
- **Monthly sales aggregation** ✅
- **Data enrichment** ✅
- **Price categorization** ✅

---

## 📂 Project Structure
```
📁 data/
📁 src/
 ├── crosscutting/         # Utility functions & logging
 ├── data_models/          # Schema definitions
 ├── data_processing/      # Data loading & transformation logic
 ├── tests/                # Unit tests
 ├── main.py               # Entry point (optional)
📁 tests/
```

---

## 📜 License
This project is licensed under the **MIT License**.

---


## 👨‍💼 Authors
Joao Teixeira (joaopteixeira58@gmail.com)

---
