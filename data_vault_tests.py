
import unittest
import pandas as pd
import os
import glob
from data_vault_builder import DataVaultBuilder

class TestDataVault(unittest.TestCase):
    def setUp(self):
        self.output_dir = 'data_vault'
        self.data_dir = '.'
        # Ensure we have data
        if not os.path.exists(self.output_dir):
            builder = DataVaultBuilder(data_dir=self.data_dir, output_dir=self.output_dir)
            builder.run_all()

    def test_hub_well_uniqueness(self):
        df = pd.read_parquet(os.path.join(self.output_dir, 'hub_well.parquet'))
        self.assertEqual(len(df), len(df['well_hash_key'].unique()), "Hub Well Hash Keys must be unique")
        self.assertEqual(len(df), len(df['well_id'].unique()), "Hub Well IDs must be unique")

    def test_hub_survey_uniqueness(self):
        df = pd.read_parquet(os.path.join(self.output_dir, 'hub_survey.parquet'))
        self.assertEqual(len(df), len(df['survey_hash_key'].unique()), "Hub Survey Hash Keys must be unique")
        self.assertEqual(len(df), len(df['survey_type_id'].unique()), "Hub Survey IDs must be unique")

    def test_hub_sensor_uniqueness(self):
        df = pd.read_parquet(os.path.join(self.output_dir, 'hub_sensor.parquet'))
        self.assertEqual(len(df), len(df['sensor_hash_key'].unique()), "Hub Sensor Hash Keys must be unique")
        self.assertEqual(len(df), len(df['sensor_id'].unique()), "Hub Sensor IDs must be unique")

    def test_link_seismic_referential_integrity(self):
        link_df = pd.read_parquet(os.path.join(self.output_dir, 'link_seismic_trace.parquet'))
        hub_well = pd.read_parquet(os.path.join(self.output_dir, 'hub_well.parquet'))
        hub_survey = pd.read_parquet(os.path.join(self.output_dir, 'hub_survey.parquet'))
        hub_sensor = pd.read_parquet(os.path.join(self.output_dir, 'hub_sensor.parquet'))

        # Check if all keys in Link exist in Hubs
        self.assertTrue(link_df['well_hash_key'].isin(hub_well['well_hash_key']).all(), "Link Well Keys must exist in Hub Well")
        self.assertTrue(link_df['survey_hash_key'].isin(hub_survey['survey_hash_key']).all(), "Link Survey Keys must exist in Hub Survey")
        self.assertTrue(link_df['sensor_hash_key'].isin(hub_sensor['sensor_hash_key']).all(), "Link Sensor Keys must exist in Hub Sensor")

    def test_link_legacy_referential_integrity(self):
        link_df = pd.read_parquet(os.path.join(self.output_dir, 'link_legacy_trace.parquet'))
        hub_well = pd.read_parquet(os.path.join(self.output_dir, 'hub_well.parquet'))
        hub_survey = pd.read_parquet(os.path.join(self.output_dir, 'hub_survey.parquet'))

        self.assertTrue(link_df['well_hash_key'].isin(hub_well['well_hash_key']).all(), "Link Well Keys must exist in Hub Well")
        self.assertTrue(link_df['survey_hash_key'].isin(hub_survey['survey_hash_key']).all(), "Link Survey Keys must exist in Hub Survey")

    def test_sat_seismic_counts(self):
        # Check if counts match source roughly (deduplication might occur, but raw rows should match if PK is unique enough)
        # Source: archive_batch_seismic_readings*.parquet
        source_files = glob.glob(os.path.join(self.data_dir, 'track_1_forensics/archive_batch_seismic_readings*.parquet'))
        total_source_rows = 0
        for f in source_files:
            try:
                # Skip corrupted file if checked in builder, but here we can just try/except
                # The builder skips corrupted files, so we should also check validity or rely on builder consistency.
                # Since we know one file is corrupt, let's verify if we can read it.
                with open(f, 'rb') as file_obj:
                    file_obj.seek(-4, 2)
                    if file_obj.read() != b'PAR1':
                        continue
                df = pd.read_parquet(f)
                total_source_rows += len(df)
            except:
                pass

        sat_df = pd.read_parquet(os.path.join(self.output_dir, 'sat_seismic_reading.parquet'))

        # We expect sat rows <= source rows (dedup). But here every reading is likely unique in timestamp/depth
        # Check if sat_df has at least as many rows as source (it shouldn't be more, but due to append logic in builder it might be if run multiple times)
        # Since I overwrite/dedup in builder, it should be equal if no dups in source.
        self.assertEqual(len(sat_df), total_source_rows, "Satellite row count should match valid source row count")

    def test_legacy_counts_aggregate(self):
        # Sum of rows in legacy files
        legacy_files = glob.glob(os.path.join(self.data_dir, 'track_1_forensics/legacy_survey_*.parquet'))
        total_source_rows = 0
        for f in legacy_files:
            try:
                df = pd.read_parquet(f)
                total_source_rows += len(df)
            except:
                pass

        sat_df = pd.read_parquet(os.path.join(self.output_dir, 'sat_legacy_reading.parquet'))
        self.assertEqual(len(sat_df), total_source_rows, "Legacy Satellite row count should match total source rows")

if __name__ == '__main__':
    unittest.main()
