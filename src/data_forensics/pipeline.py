import logging
from .config import Config
from .sgx_converter import SGXConverter
from .data_lake import DataLakeManager
from .parquet_repair import ParquetDoctor
from .utils import setup_logger

logger = logging.getLogger("data_forensics")

class ForensicsPipeline:
    def __init__(self):
        self.config = Config()
        setup_logger(level=self.config.log_level)
        self.sgx_converter = SGXConverter(self.config)
        self.lake_manager = DataLakeManager(self.config)
        self.doctor = ParquetDoctor(self.config)

    def run(self):
        logger.info("Starting Forensics Pipeline...")

        # Step 1: Convert SGX to Parquet
        logger.info("--- Step 1: SGX Conversion ---")
        self.sgx_converter.convert_all()

        # Step 2: Ingest to Data Lake
        logger.info("--- Step 2: Data Lake Ingestion ---")
        self.lake_manager.move_parquet_files()

        # Step 3: Validate Initial State
        logger.info("--- Step 3: Validation ---")
        self.lake_manager.validate_parquet_files()

        # Step 4: Repair Corrupted File
        logger.info("--- Step 4: Repairing Corrupted Data ---")
        success = self.doctor.repair_corrupted_file()

        if success:
            logger.info("Repair successful.")
        else:
            logger.error("Repair failed.")

        # Step 5: Final Validation
        logger.info("--- Step 5: Final Validation ---")
        self.lake_manager.validate_parquet_files()

        logger.info("Pipeline completed.")

if __name__ == "__main__":
    pipeline = ForensicsPipeline()
    pipeline.run()
