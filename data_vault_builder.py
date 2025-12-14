
import pandas as pd
import hashlib
import os
import glob
from datetime import datetime
import warnings

# Suppress pyarrow warnings that might clutter output
warnings.simplefilter(action='ignore', category=FutureWarning)

class DataVaultBuilder:
    def __init__(self, data_dir='.', output_dir='data_vault'):
        self.data_dir = data_dir
        self.output_dir = output_dir
        self.load_date = datetime.now()
        os.makedirs(self.output_dir, exist_ok=True)
        print(f"Data Vault Builder initialized. Output directory: {self.output_dir}")

    def generate_hash_key(self, *args):
        """Generates an MD5 hash key from the provided arguments."""
        # Standardize input: convert to string, strip whitespace, uppercase
        raw_key = "".join([str(val).strip().upper() for val in args])
        return hashlib.md5(raw_key.encode('utf-8')).hexdigest()

    def get_file_checksum(self, filepath):
        """Calculates MD5 checksum of a file."""
        hash_md5 = hashlib.md5()
        try:
            with open(filepath, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            print(f"Error calculating checksum for {filepath}: {e}")
            return None

    def _save_table(self, df, table_name, pk=None):
        """Saves a dataframe to a parquet file in the output directory.
        Args:
            df: DataFrame to save
            table_name: Name of the table (file)
            pk: List of columns to use as Primary Key for deduplication.
                If None, uses all columns for deduplication.
        """
        output_path = os.path.join(self.output_dir, f"{table_name}.parquet")

        if os.path.exists(output_path):
             existing_df = pd.read_parquet(output_path)
             combined_df = pd.concat([existing_df, df], ignore_index=True)

             if pk:
                 combined_df = combined_df.drop_duplicates(subset=pk)
             else:
                 combined_df = combined_df.drop_duplicates()

             combined_df.to_parquet(output_path, index=False)
        else:
            if pk:
                df = df.drop_duplicates(subset=pk)
            else:
                df = df.drop_duplicates()
            df.to_parquet(output_path, index=False)
        print(f"Saved {table_name} with {len(df)} rows (or appended/updated).")

    def ingest_masters(self):
        """Ingests Master CSV files into Hubs and Master Satellites."""
        print("Ingesting Masters...")

        # --- Wells ---
        wells_path = os.path.join(self.data_dir, 'master_wells.csv')
        if os.path.exists(wells_path):
            df_wells = pd.read_csv(wells_path)
            checksum = self.get_file_checksum(wells_path)

            # Hub Well
            df_wells['well_hash_key'] = df_wells['well_id'].apply(lambda x: self.generate_hash_key(x))
            df_wells['load_date'] = self.load_date
            df_wells['record_source'] = 'master_wells.csv'

            hub_well = df_wells[['well_hash_key', 'well_id', 'load_date', 'record_source']]
            self._save_table(hub_well, 'hub_well', pk=['well_hash_key'])

            # Sat Well Details
            attr_cols = ['well_name', 'location_lat', 'location_long', 'operator', 'spud_date']
            df_wells['hash_diff'] = df_wells.apply(lambda row: self.generate_hash_key(*[row[c] for c in attr_cols]), axis=1)
            df_wells['file_checksum'] = checksum

            sat_well = df_wells[['well_hash_key'] + attr_cols + ['load_date', 'record_source', 'hash_diff', 'file_checksum']]
            self._save_table(sat_well, 'sat_well_details', pk=['well_hash_key', 'hash_diff'])

        # --- Surveys ---
        surveys_path = os.path.join(self.data_dir, 'master_surveys.csv')
        if os.path.exists(surveys_path):
            df_surveys = pd.read_csv(surveys_path)
            checksum = self.get_file_checksum(surveys_path)

            # Hub Survey
            df_surveys['survey_hash_key'] = df_surveys['survey_type_id'].apply(lambda x: self.generate_hash_key(x))
            df_surveys['load_date'] = self.load_date
            df_surveys['record_source'] = 'master_surveys.csv'

            hub_survey = df_surveys[['survey_hash_key', 'survey_type_id', 'load_date', 'record_source']]
            self._save_table(hub_survey, 'hub_survey', pk=['survey_hash_key'])

            # Sat Survey Details
            attr_cols = ['survey_type']
            df_surveys['hash_diff'] = df_surveys.apply(lambda row: self.generate_hash_key(*[row[c] for c in attr_cols]), axis=1)
            df_surveys['file_checksum'] = checksum

            sat_survey = df_surveys[['survey_hash_key'] + attr_cols + ['load_date', 'record_source', 'hash_diff', 'file_checksum']]
            self._save_table(sat_survey, 'sat_survey_details', pk=['survey_hash_key', 'hash_diff'])

        # --- Sensors ---
        sensors_path = os.path.join(self.data_dir, 'master_sensors.csv')
        if os.path.exists(sensors_path):
            df_sensors = pd.read_csv(sensors_path)
            checksum = self.get_file_checksum(sensors_path)

            # Hub Sensor
            df_sensors['sensor_hash_key'] = df_sensors['sensor_id'].apply(lambda x: self.generate_hash_key(x))
            df_sensors['load_date'] = self.load_date
            df_sensors['record_source'] = 'master_sensors.csv'

            hub_sensor = df_sensors[['sensor_hash_key', 'sensor_id', 'load_date', 'record_source']]
            self._save_table(hub_sensor, 'hub_sensor', pk=['sensor_hash_key'])

            # Sat Sensor Details
            attr_cols = ['sensor_type', 'calibration_date']
            df_sensors['hash_diff'] = df_sensors.apply(lambda row: self.generate_hash_key(*[row[c] for c in attr_cols]), axis=1)
            df_sensors['file_checksum'] = checksum

            sat_sensor = df_sensors[['sensor_hash_key'] + attr_cols + ['load_date', 'record_source', 'hash_diff', 'file_checksum']]
            self._save_table(sat_sensor, 'sat_sensor_details', pk=['sensor_hash_key', 'hash_diff'])

    def ingest_archive_batch(self):
        """Ingests Archive Batch Seismic Readings."""
        print("Ingesting Archive Batches...")
        archive_files = glob.glob(os.path.join(self.data_dir, 'track_1_forensics/archive_batch_seismic_readings*.parquet'))

        for file_path in archive_files:
            try:
                # Basic check for magic bytes
                with open(file_path, 'rb') as f:
                    f.seek(-4, 2)
                    if f.read() != b'PAR1':
                        print(f"Skipping corrupted file: {file_path}")
                        continue

                df = pd.read_parquet(file_path)
                filename = os.path.basename(file_path)
                checksum = self.get_file_checksum(file_path)
                print(f"Processing {filename} with {len(df)} rows.")

                # Hub Well
                df['well_hash_key'] = df['well_id'].apply(lambda x: self.generate_hash_key(x))
                hub_well = df[['well_hash_key', 'well_id']].drop_duplicates()
                hub_well['load_date'] = self.load_date
                hub_well['record_source'] = filename
                self._save_table(hub_well, 'hub_well', pk=['well_hash_key'])

                # Hub Survey
                df['survey_hash_key'] = df['survey_type_id'].apply(lambda x: self.generate_hash_key(x))
                hub_survey = df[['survey_hash_key', 'survey_type_id']].drop_duplicates()
                hub_survey['load_date'] = self.load_date
                hub_survey['record_source'] = filename
                self._save_table(hub_survey, 'hub_survey', pk=['survey_hash_key'])

                # Hub Sensor
                df['sensor_hash_key'] = df['sensor_id'].apply(lambda x: self.generate_hash_key(x))
                hub_sensor = df[['sensor_hash_key', 'sensor_id']].drop_duplicates()
                hub_sensor['load_date'] = self.load_date
                hub_sensor['record_source'] = filename
                self._save_table(hub_sensor, 'hub_sensor', pk=['sensor_hash_key'])

                # Link Seismic Observation
                df['link_hash_key'] = df.apply(lambda row: self.generate_hash_key(row['well_id'], row['survey_type_id'], row['sensor_id']), axis=1)

                link_obs = df[['link_hash_key', 'well_hash_key', 'survey_hash_key', 'sensor_hash_key']].drop_duplicates()
                link_obs['load_date'] = self.load_date
                link_obs['record_source'] = filename
                self._save_table(link_obs, 'link_seismic_trace', pk=['link_hash_key'])

                # Sat Seismic Reading
                # Rename timestamp to reading_timestamp
                df.rename(columns={'timestamp': 'reading_timestamp'}, inplace=True)
                attr_cols = ['depth_ft', 'amplitude', 'reading_timestamp', 'quality_flag']

                df['hash_diff'] = df.apply(lambda row: self.generate_hash_key(*[row[c] for c in attr_cols]), axis=1)
                df['file_checksum'] = checksum
                df['load_date'] = self.load_date
                df['record_source'] = filename

                sat_reading = df[['link_hash_key'] + attr_cols + ['load_date', 'record_source', 'hash_diff', 'file_checksum']]
                # Satellites should not dedup on just Link Key!
                # We let it default to all columns or pk=None
                self._save_table(sat_reading, 'sat_seismic_reading', pk=None)

            except Exception as e:
                print(f"Failed to process {file_path}: {e}")

    def ingest_legacy_surveys(self):
        """Ingests Legacy Survey files."""
        print("Ingesting Legacy Surveys...")
        legacy_files = glob.glob(os.path.join(self.data_dir, 'track_1_forensics/legacy_survey_*.parquet'))

        for file_path in legacy_files:
            try:
                df = pd.read_parquet(file_path)
                filename = os.path.basename(file_path)
                checksum = self.get_file_checksum(file_path)

                parts = filename.replace('.parquet', '').split('_')
                if len(parts) >= 3:
                    year = parts[2]
                else:
                    year = 'UNKNOWN'

                print(f"Processing {filename} (Year: {year}) with {len(df)} rows.")

                # Hub Well
                df['well_hash_key'] = df['well_id'].apply(lambda x: self.generate_hash_key(x))
                hub_well = df[['well_hash_key', 'well_id']].drop_duplicates()
                hub_well['load_date'] = self.load_date
                hub_well['record_source'] = filename
                self._save_table(hub_well, 'hub_well', pk=['well_hash_key'])

                # Hub Survey
                df.rename(columns={'survey_id': 'survey_type_id'}, inplace=True)
                df['survey_hash_key'] = df['survey_type_id'].apply(lambda x: self.generate_hash_key(x))
                hub_survey = df[['survey_hash_key', 'survey_type_id']].drop_duplicates()
                hub_survey['load_date'] = self.load_date
                hub_survey['record_source'] = filename
                self._save_table(hub_survey, 'hub_survey', pk=['survey_hash_key'])

                # Link Legacy Trace
                df['link_hash_key'] = df.apply(lambda row: self.generate_hash_key(row['well_id'], row['survey_type_id']), axis=1)

                link_legacy = df[['link_hash_key', 'well_hash_key', 'survey_hash_key']].drop_duplicates()
                link_legacy['load_date'] = self.load_date
                link_legacy['record_source'] = filename
                self._save_table(link_legacy, 'link_legacy_trace', pk=['link_hash_key'])

                # Sat Legacy Reading
                df['year'] = year
                attr_cols = ['depth', 'amplitude', 'quality', 'year']
                df['hash_diff'] = df.apply(lambda row: self.generate_hash_key(*[row[c] for c in attr_cols]), axis=1)
                df['file_checksum'] = checksum
                df['load_date'] = self.load_date
                df['record_source'] = filename

                sat_legacy = df[['link_hash_key'] + attr_cols + ['load_date', 'record_source', 'hash_diff', 'file_checksum']]
                self._save_table(sat_legacy, 'sat_legacy_reading', pk=None)

            except Exception as e:
                print(f"Failed to process {file_path}: {e}")

    def run_all(self):
        self.ingest_masters()
        self.ingest_archive_batch()
        self.ingest_legacy_surveys()
        print("Ingestion Complete.")

if __name__ == "__main__":
    builder = DataVaultBuilder()
    builder.run_all()
