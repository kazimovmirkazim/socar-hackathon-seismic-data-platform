import os
import struct
import pandas as pd
import shutil
import logging
from pathlib import Path
from typing import Optional, Tuple

from .config import Config

logger = logging.getLogger("data_forensics")

class ParquetDoctor:
    """Handles analysis and repair of corrupted Parquet files."""

    def __init__(self, config: Config):
        self.config = config

    def repair_corrupted_file(self) -> bool:
        """Orchestrates the repair of the specific corrupted file."""
        file_path = self.config.data_lake_raw / self.config.corrupted_file_name

        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            return False

        logger.info(f"Starting repair for {file_path}")

        # 1. Analyze file
        with open(file_path, 'rb') as f:
            data = f.read()
        logger.info(f"File size: {len(data)} bytes")

        if data[:4] != b'PAR1':
            logger.error("Header is not PAR1!")
            return False

        # 2. Find PAR1 markers
        par1_positions = []
        for i in range(len(data) - 3):
            if data[i:i+4] == b'PAR1':
                par1_positions.append(i)

        logger.info(f"PAR1 positions: {par1_positions}")

        if len(par1_positions) < 2:
            logger.error("Not enough PAR1 markers found.")
            return False

        # 3. Find FLAG
        flag_start = data.find(b'FLAG{')
        if flag_start != -1:
            flag_content = data[flag_start:].decode('utf-8', errors='ignore').strip()
            # Clean up null bytes from the end if any
            flag_content = flag_content.split('}')[0] + '}'
            logger.info(f"{'='*50}")
            logger.info(f" FLAG FOUND: {flag_content}")
            logger.info(f"{'='*50}")
        else:
            logger.warning("FLAG not found.")

        # 4. Reconstruct Valid Parquet
        # The logic from the notebook (cell 27):
        # Second PAR1 is at 40040. Metadata length is 4 bytes before that.
        second_par1_pos = 40040 # Hardcoded based on notebook analysis, or dynamic if we use par1_positions[1]

        if len(par1_positions) >= 2:
            second_par1_pos = par1_positions[1]

        metadata_length_pos = second_par1_pos - 4
        if metadata_length_pos < 0:
             logger.error("Invalid metadata length position.")
             return False

        metadata_length_bytes = data[metadata_length_pos:metadata_length_pos+4]
        metadata_length = struct.unpack('<I', metadata_length_bytes)[0]
        logger.info(f"Metadata length: {metadata_length}")

        metadata_start = metadata_length_pos - metadata_length
        logger.info(f"Metadata starts at: {metadata_start}")

        valid_end = second_par1_pos + 4
        valid_parquet = data[:valid_end]

        clean_path = self.config.data_lake_raw / (file_path.stem + self.config.clean_suffix)

        with open(clean_path, 'wb') as f:
            f.write(valid_parquet)

        logger.info(f"Created clean file: {clean_path}")

        # 5. Verify Repair
        try:
            df = pd.read_parquet(clean_path)
            logger.info(f"Verification Success! Rows: {len(df)}")
            logger.debug(f"Columns: {df.columns.tolist()}")
        except Exception as e:
            logger.error(f"Verification failed: {e}")
            return False

        # 6. Replace and Backup
        backup_path = self.config.data_lake_raw / (file_path.stem + self.config.backup_suffix)
        shutil.copy(file_path, backup_path)
        logger.info(f"Backup created at {backup_path}")

        try:
            os.remove(file_path)
            shutil.copy(clean_path, file_path)
            logger.info("Replaced corrupted file with clean version.")
        except Exception as e:
            logger.error(f"Failed to replace file: {e}")
            return False

        return True

    def recover_from_raw(self):
        """Alternative recovery method: treating as raw SGX-like stream (Cell 30)."""
        # Note: The notebook has a separate script block for this, but the primary repair method
        # seemed to be the truncation/footer fix. The "recover_archive_batch_from_raw.py" cell
        # implies that sometimes the file is just raw bytes without parquet structure,
        # but the main repair logic used the PAR1 markers.
        # I will implement this as a fallback or separate method if needed.
        # For now, the repair_corrupted_file method above implements the logic that was successful (Cell 27).
        pass
