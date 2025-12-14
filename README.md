# Project from DataXKEy: Caspian Basin Data Restoration

> **Mission**: End-to-End Restoration & Orchestration of Caspian Basin Data Systems.
> **Objective**: Transform the legacy of the Caspian Basin from a state of "silent" and corrupted data into a high-performance, real-time intelligence ecosystem.

## Overview

Project FELINA is a comprehensive data engineering response to the "Seismic Reckoning." It involves recovering decades of seismic data from disparate legacy systems, modeling it into an immutable Data Vault, transforming it into actionable analytics, and serving it via a robust orchestration platform.

This repository contains the code for all four tracks of the challenge:
1.  **Forensics**: Recovering corrupted Parquet files and decoding ghost formats (.sgx).
2.  **Data Vault**: Building a Raw Vault to preserve provenance and history.
3.  **Analytics**: Creating a Star Schema and Dashboards for decision support.
4.  **Platform**: Orchestrating the pipeline with Airflow and Docker.

---

## Track 1: The Parquet Enigma & Ghost Format (Forensics)

Located in `src/data_forensics/`.

We developed a specialized Python package to handle forensic data recovery.

*   **Parquet Recovery**: The module `parquet_repair.py` (via `ParquetDoctor`) detects and repairs corrupted Parquet files (specifically `archive_batch_seismic_readings.parquet`) by reconstructing metadata footers and extracting hidden flags.
*   **SGX Conversion**: The `sgx_converter.py` module decodes the proprietary binary `.sgx` format, identifying headers and trace data, and converting them into modern Parquet format for ingestion.
*   **Data Lake**: The pipeline organizes recovered files into a Raw Data Lake zone.

**Usage:**
```bash
python run.py
```

---

## Track 2: The Path to Insight (Data Vault)

Located in `data_vault_builder.py` and `data_vault_tests.py`.

To ensure data lineage and auditability, we implemented a Data Vault 2.0 architecture.

*   **Hubs**: Unique lists of business keys.
    *   `hub_well`: Well IDs.
    *   `hub_survey`: Survey Types.
    *   `hub_sensor`: Sensor IDs.
*   **Links**: Associations between business entities.
    *   `link_seismic_trace`: Connects Well, Survey, and Sensor.
    *   `link_legacy_trace`: Connects Well and Survey (for legacy data).
*   **Satellites**: Descriptive data with history and provenance.
    *   `sat_seismic_reading`: High-volume sensor readings with timestamps, depth, and amplitude.
    *   `sat_well_details`, `sat_survey_details`, `sat_sensor_details`.
    *   **Provenance Columns**: `record_source`, `load_date`, `file_checksum`.

**Validation:**
Unit tests in `data_vault_tests.py` verify referential integrity, uniqueness, and row counts against the source data.

**Usage:**
```bash
python data_vault_builder.py
python data_vault_tests.py
```

---

## Track 3: Transforming into Analytics

Located in `analytics_etl.py` and `dashboard.ipynb`.

We transformed the Data Vault into a Dimensional Model (Star Schema) optimized for querying and visualization.

*   **Dimensions**: `dim_well`, `dim_sensor`, `dim_survey`, `dim_strata` (derived from depth).
*   **Facts**:
    *   `fact_sensor_readings`: Granular readings partitioned by source.
    *   `fact_anomalies`: Flagged quality failures.
    *   `fact_survey_events`: Aggregated trace sessions.
*   **Data Marts**:
    *   `mart_well_performance`: KPI aggregates per well.
    *   `mart_sensor_analysis`: Sensor reliability metrics.
    *   `mart_survey_summary`: Survey acquisition stats.
*   **Technology**: We use **Delta Lake** (via `deltalake` python package) where available for ACID transactions and Time Travel, falling back to Parquet otherwise.

**Dashboard**:
The `dashboard.ipynb` notebook provides:
*   Interactive Map of Wells (Folium).
*   Well Performance Distribution (Seaborn).
*   Sensor Reliability Analysis.
*   Amplitude vs Depth Heatmaps.

**Usage:**
```bash
python analytics_etl.py
# Open dashboard.ipynb in Jupyter
```

---

## Track 4: Build the Platform

Located in `pipeline_setup/`.

The entire end-to-end pipeline is containerized and orchestrated using Apache Airflow.

*   **Infrastructure**: Docker Compose (`docker-compose.yaml`) spins up:
    *   Airflow Webserver, Scheduler, and Postgres DB.
    *   Mounts the project root to share code and data.
*   **Orchestration**: The DAG `seismic_data_pipeline.py` defines the workflow:
    1.  `ingest_data_vault`: Triggers the Data Vault Builder.
    2.  `transform_analytics`: Triggers the Analytics ETL.
    3.  Dependencies ensure correct execution order.

**Usage:**
```bash
cd pipeline_setup
docker-compose up --build
# Access Airflow at http://localhost:8080
```

---

## How to Run End-to-End

1.  **Install Dependencies**:
    ```bash
    pip install pandas pyarrow deltalake
    ```

2.  **Run Forensics**:
    ```bash
    python run.py
    ```
    *This prepares the raw data in `track_1_forensics/`.*

3.  **Build Data Vault**:
    ```bash
    python data_vault_builder.py
    ```
    *This creates the `data_vault/` directory.*

4.  **Run Tests**:
    ```bash
    python data_vault_tests.py
    ```

5.  **Run Analytics ETL**:
    ```bash
    python analytics_etl.py
    ```
    *This creates the `analytics/` directory (Delta Tables).*

6.  **View Dashboard**:
    Open `dashboard.ipynb` and run all cells to see the visualizations.
