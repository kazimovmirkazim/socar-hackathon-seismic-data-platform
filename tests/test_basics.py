import unittest
from pathlib import Path
from src.data_forensics.config import Config
from src.data_forensics.utils import setup_logger

class TestConfig(unittest.TestCase):
    def test_config_paths(self):
        config = Config()
        self.assertIsInstance(config.source_folder, Path)
        self.assertIsInstance(config.data_lake_raw, Path)
        self.assertEqual(config.corrupted_file_name, "archive_batch_seismic_readings.parquet")

class TestUtils(unittest.TestCase):
    def test_logger(self):
        logger = setup_logger(name="test_logger", level="DEBUG")
        self.assertEqual(logger.name, "test_logger")
        self.assertEqual(logger.level, 10) # DEBUG is 10

if __name__ == '__main__':
    unittest.main()
