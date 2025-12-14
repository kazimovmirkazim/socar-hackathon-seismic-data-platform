import shutil
import os
from pathlib import Path
import logging
from typing import List

from .config import Config

logger = logging.getLogger("data_forensics")

class DataLakeManager:
    """Manages file operations in the Data Lake."""

    def __init__(self, config: Config):
        self.config = config

    def move_parquet_files(self):
        """Moves all Parquet files from source to data lake raw folder."""
        src_folder = self.config.source_folder
        datalake_raw = self.config.data_lake_raw

        # Ensure destination exists
        datalake_raw.mkdir(parents=True, exist_ok=True)

        moved_count = 0
        for f in os.listdir(src_folder):
            if f.endswith(".parquet"):
                src_path = src_folder / f
                dst_path = datalake_raw / f
                try:
                    shutil.copy(src_path, dst_path)
                    moved_count += 1
                except Exception as e:
                    logger.error(f"Failed to copy {f}: {e}")

        logger.info(f"Moved {moved_count} Parquet files to {datalake_raw}")

    def validate_parquet_files(self) -> List[Path]:
        """Checks which parquet files in the data lake are valid."""
        folder = self.config.data_lake_raw
        files = [f for f in os.listdir(folder) if f.endswith(".parquet")]
        valid_files = []

        import pyarrow.parquet as pq

        for f in files:
            path = folder / f
            try:
                pq.read_table(path)
                valid_files.append(path)
            except Exception as e:
                logger.warning(f"Skipped invalid Parquet: {f} -> {str(e)[:100]}")

        logger.info(f"Found {len(valid_files)} valid Parquet files out of {len(files)}")
        return valid_files
