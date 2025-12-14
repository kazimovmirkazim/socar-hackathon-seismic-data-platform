
import pandas as pd
import os
import shutil
import warnings
from datetime import datetime
try:
    from deltalake import write_deltalake
    HAS_DELTA = True
except ImportError:
    HAS_DELTA = False
    print("Deltalake library not found. Falling back to Parquet.")

# Suppress warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

class AnalyticsETL:
    def __init__(self, input_dir='data_vault', output_dir='analytics'):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.load_date = datetime.now()
        os.makedirs(self.output_dir, exist_ok=True)
        print(f"Analytics ETL initialized. Input: {self.input_dir}, Output: {self.output_dir}")

    def _load_table(self, table_name):
        path = os.path.join(self.input_dir, f"{table_name}.parquet")
        if os.path.exists(path):
            return pd.read_parquet(path)
        else:
            print(f"Warning: Table {table_name} not found.")
            return pd.DataFrame()

    def _save_table(self, df, table_name, partition_cols=None, mode='overwrite'):
        """Saves DataFrame as Delta Table (Time Travel support) or Parquet."""
        output_path = os.path.join(self.output_dir, table_name)

        if HAS_DELTA:
            try:
                write_deltalake(output_path, df, mode=mode, partition_by=partition_cols)
                print(f"Saved {table_name} as Delta Table.")
            except Exception as e:
                print(f"Error writing Delta table {table_name}: {e}. Falling back to Parquet.")
                # Fallback to parquet
                output_path_pq = output_path + ".parquet"
                df.to_parquet(output_path_pq, index=False)
                print(f"Saved {table_name} as Parquet.")
        else:
            output_path_pq = output_path + ".parquet"
            df.to_parquet(output_path_pq, index=False)
            print(f"Saved {table_name} as Parquet.")

    def create_dimensions(self):
        print("Creating Dimensions...")

        # --- Dim Well ---
        hub_well = self._load_table('hub_well')
        sat_well = self._load_table('sat_well_details')

        if not hub_well.empty and not sat_well.empty:
            # Get latest satellite record for each well
            sat_well_latest = sat_well.sort_values('load_date').groupby('well_hash_key').tail(1)
            dim_well = pd.merge(hub_well, sat_well_latest, on='well_hash_key', how='left', suffixes=('', '_sat'))

            # Select and Rename columns
            dim_well = dim_well[['well_hash_key', 'well_id', 'well_name', 'location_lat', 'location_long', 'operator', 'spud_date']]
            self._save_table(dim_well, 'dim_well')
            self.dim_well = dim_well # Cache for facts
        else:
             self.dim_well = pd.DataFrame()

        # --- Dim Sensor ---
        hub_sensor = self._load_table('hub_sensor')
        sat_sensor = self._load_table('sat_sensor_details')

        if not hub_sensor.empty and not sat_sensor.empty:
            sat_sensor_latest = sat_sensor.sort_values('load_date').groupby('sensor_hash_key').tail(1)
            dim_sensor = pd.merge(hub_sensor, sat_sensor_latest, on='sensor_hash_key', how='left')

            dim_sensor = dim_sensor[['sensor_hash_key', 'sensor_id', 'sensor_type', 'calibration_date']]
            self._save_table(dim_sensor, 'dim_sensor')
            self.dim_sensor = dim_sensor
        else:
            self.dim_sensor = pd.DataFrame()

        # --- Dim Survey ---
        hub_survey = self._load_table('hub_survey')
        sat_survey = self._load_table('sat_survey_details')

        if not hub_survey.empty and not sat_survey.empty:
            sat_survey_latest = sat_survey.sort_values('load_date').groupby('survey_hash_key').tail(1)
            dim_survey = pd.merge(hub_survey, sat_survey_latest, on='survey_hash_key', how='left')

            dim_survey = dim_survey[['survey_hash_key', 'survey_type_id', 'survey_type']]
            self._save_table(dim_survey, 'dim_survey')
            self.dim_survey = dim_survey
        else:
            self.dim_survey = pd.DataFrame()

        # --- Dim Strata (Derived) ---
        # Since we don't have a source, we create a static dimension for reference
        strata_data = [
            {'strata_id': 1, 'strata_name': 'Shallow', 'min_depth': 0, 'max_depth': 2000},
            {'strata_id': 2, 'strata_name': 'Intermediate', 'min_depth': 2000, 'max_depth': 5000},
            {'strata_id': 3, 'strata_name': 'Deep', 'min_depth': 5000, 'max_depth': 99999},
        ]
        dim_strata = pd.DataFrame(strata_data)
        self._save_table(dim_strata, 'dim_strata')
        self.dim_strata = dim_strata

        # --- Dim Time ---
        # Will be generated based on fact table range later or dynamically
        # For now, we can create a simple one or rely on timestamps in facts

    def get_strata_id(self, depth):
        if pd.isna(depth): return -1
        if depth < 2000: return 1
        elif depth < 5000: return 2
        else: return 3

    def create_facts(self):
        print("Creating Facts...")

        # Load Link and Sat for Readings
        link_trace = self._load_table('link_seismic_trace')
        sat_reading = self._load_table('sat_seismic_reading')

        if link_trace.empty or sat_reading.empty:
            print("Missing link or satellite data for readings.")
            return

        # Join Link and Sat
        # Both might have 'record_source' and 'load_date'. specify suffixes to avoid confusion
        fact_readings = pd.merge(sat_reading, link_trace, on='link_hash_key', how='inner', suffixes=('', '_link'))

        # Add Strata ID
        fact_readings['strata_id'] = fact_readings['depth_ft'].apply(self.get_strata_id)

        # Clean up
        cols_to_keep = [
            'link_hash_key', 'well_hash_key', 'survey_hash_key', 'sensor_hash_key',
            'reading_timestamp', 'depth_ft', 'amplitude', 'quality_flag', 'strata_id',
            'record_source'
        ]

        # Verify columns exist
        missing_cols = [c for c in cols_to_keep if c not in fact_readings.columns]
        if missing_cols:
            print(f"Warning: Missing columns in fact_readings: {missing_cols}")
            print("Available columns:", fact_readings.columns)

        fact_readings = fact_readings[cols_to_keep]

        # Save Fact Sensor Readings
        # Partitioning by record_source helps with "Data Source Format" breakdown
        self._save_table(fact_readings, 'fact_sensor_readings', partition_cols=['record_source'])
        self.fact_readings = fact_readings

        # --- Fact Anomalies ---
        # Define anomaly: quality_flag = 0 or unusual amplitude (e.g., > 95th percentile or something,
        # but prompt says "Fact tables for... anomalies". I'll stick to quality_flag for explicit anomalies
        # and maybe high amplitude).
        # Let's assume quality_flag == 0 is an anomaly.

        fact_anomalies = fact_readings[fact_readings['quality_flag'] == 0].copy()
        fact_anomalies['anomaly_type'] = 'Quality Failure'
        self._save_table(fact_anomalies, 'fact_anomalies', partition_cols=['record_source'])

        # --- Fact Survey Events ---
        # Summarize readings into events
        # Unique combination of Well + Survey + Time (approx)?
        # Or just use the Link table as the base for Survey Events (traces)

        # Let's define a Survey Event as a specific Trace (Link)
        fact_survey_events = fact_readings.groupby(['link_hash_key', 'well_hash_key', 'survey_hash_key', 'record_source']).agg(
            num_readings=('amplitude', 'count'),
            avg_amplitude=('amplitude', 'mean'),
            start_time=('reading_timestamp', 'min'),
            end_time=('reading_timestamp', 'max')
        ).reset_index()

        self._save_table(fact_survey_events, 'fact_survey_events')
        self.fact_survey_events = fact_survey_events

    def create_marts(self):
        print("Creating Data Marts...")

        if not hasattr(self, 'fact_readings'):
            print("Facts not created yet. Skipping Marts.")
            return

        # --- Mart Well Performance ---
        # Summarizes well performance. Calculates total readings, average amplitude,
        # and data quality rate for each well, broken down by data source format

        # Join with Dim Well to get well names
        df = pd.merge(self.fact_readings, self.dim_well[['well_hash_key', 'well_id']], on='well_hash_key', how='left')

        mart_well = df.groupby(['well_id', 'record_source']).agg(
            total_readings=('amplitude', 'count'),
            avg_amplitude=('amplitude', 'mean'),
            quality_rate=('quality_flag', 'mean') # Assuming 1 is good, 0 is bad
        ).reset_index()

        self._save_table(mart_well, 'mart_well_performance')

        # --- Mart Sensor Analysis ---
        # Summarizes sensor reliability. Calculates total readings, data quality rate,
        # and average amplitude for each sensor type.

        # Join with Dim Sensor
        df_sens = pd.merge(self.fact_readings, self.dim_sensor[['sensor_hash_key', 'sensor_type']], on='sensor_hash_key', how='left')

        mart_sensor = df_sens.groupby(['sensor_type']).agg(
            total_readings=('amplitude', 'count'),
            avg_amplitude=('amplitude', 'mean'),
            quality_rate=('quality_flag', 'mean')
        ).reset_index()

        self._save_table(mart_sensor, 'mart_sensor_analysis')

        # --- Mart Survey Summary ---
        # Summarizes data acquisition for different survey types. Counts wells surveyed,
        # total readings, average amplitude, and timestamps, also breaking down data source formats.

        # Join with Dim Survey and Dim Well (to count wells)
        df_surv = pd.merge(self.fact_readings, self.dim_survey[['survey_hash_key', 'survey_type']], on='survey_hash_key', how='left')

        mart_survey = df_surv.groupby(['survey_type', 'record_source']).agg(
            wells_surveyed=('well_hash_key', 'nunique'),
            total_readings=('amplitude', 'count'),
            avg_amplitude=('amplitude', 'mean'),
            min_timestamp=('reading_timestamp', 'min'),
            max_timestamp=('reading_timestamp', 'max')
        ).reset_index()

        self._save_table(mart_survey, 'mart_survey_summary')


    def run(self):
        self.create_dimensions()
        self.create_facts()
        self.create_marts()
        print("Analytics ETL Complete.")

if __name__ == "__main__":
    etl = AnalyticsETL()
    etl.run()
