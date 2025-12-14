import json
import os

notebook_content = {
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Building the Data Pipeline\n",
    "\n",
    "This notebook demonstrates the setup and execution of the data pipeline for the Seismic Data project using Apache Airflow.\n",
    "\n",
    "## 1. Set up Environment\n",
    "\n",
    "We will use **Docker** and **Docker Compose** to set up the environment. This ensures a consistent runtime for Airflow and our ETL scripts.\n",
    "\n",
    "### Directory Structure\n",
    "We have organized the pipeline configuration in the `pipeline_setup/` directory:\n",
    "- `Dockerfile`: Defines the custom Airflow image with necessary Python libraries.\n",
    "- `requirements.txt`: Lists dependencies (`pandas`, `deltalake`, etc.).\n",
    "- `docker-compose.yaml`: Defines the services (Airflow Webserver, Scheduler, Postgres).\n",
    "- `dags/`: Directory containing the Airflow DAGs.\n",
    "\n",
    "### Dockerfile\n",
    "The Dockerfile extends the official Airflow image and installs our project dependencies."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "!cat pipeline_setup/Dockerfile"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "### Docker Compose\n",
    "The `docker-compose.yaml` file sets up the Airflow components. crucially, it mounts the project root directory to `/opt/airflow/project_root` inside the container, allowing the Airflow tasks to access `data_vault_builder.py`, `analytics_etl.py`, and the data files."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "!cat pipeline_setup/docker-compose.yaml"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## 2. Deploy Airflow\n",
    "\n",
    "To deploy the pipeline, navigate to the `pipeline_setup` directory and run:\n",
    "\n",
    "```bash\n",
    "docker-compose up -d --build\n",
    "```\n",
    "\n",
    "This command will:\n",
    "1. Build the custom image.\n",
    "2. Initialize the Airflow database.\n",
    "3. Start the Webserver (port 8080) and Scheduler.\n",
    "\n",
    "Access the Airflow UI at `http://localhost:8080` with credentials `airflow`/`airflow`."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## 3. Create Airflow DAG\n",
    "\n",
    "We created a DAG named `seismic_data_etl` in `pipeline_setup/dags/seismic_data_pipeline.py`. This DAG orchestrates the two main steps of our pipeline:\n",
    "1. **Ingestion**: Runs `data_vault_builder.py` to process raw CSV/Parquet files into the Data Vault.\n",
    "2. **Transformation**: Runs `analytics_etl.py` to transform Data Vault data into Analytics (Star Schema/Delta Tables)."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "!cat pipeline_setup/dags/seismic_data_pipeline.py"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## 4. Running the Pipeline\n",
    "\n",
    "Once Airflow is running:\n",
    "1. Login to the Airflow UI.\n",
    "2. Unpause the `seismic_data_etl` DAG.\n",
    "3. Trigger the DAG manually or wait for the scheduled run.\n",
    "4. Monitor the task logs to verify that `data_vault_builder.py` and `analytics_etl.py` executed successfully."
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.8.10"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}

with open("Building_Data_Pipeline.ipynb", "w") as f:
    json.dump(notebook_content, f, indent=1)
