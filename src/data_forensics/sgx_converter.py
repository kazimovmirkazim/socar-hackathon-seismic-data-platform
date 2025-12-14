import os
import struct
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import List, Dict, Optional
import logging

from .config import Config

logger = logging.getLogger("data_forensics")

class SGXConverter:
    """Handles conversion of SGX files to Parquet."""

    def __init__(self, config: Config):
        self.config = config

    def convert_all(self):
        """Converts all SGX files in the source folder to Parquet."""
        sgx_files = [f for f in os.listdir(self.config.source_folder) if f.endswith(".sgx")]
        logger.info(f"Found {len(sgx_files)} SGX files to convert.")

        for sgx in sgx_files:
            self.convert_file(sgx)

    def convert_file(self, filename: str) -> Optional[Path]:
        """Converts a single SGX file to Parquet."""
        path = self.config.source_folder / filename
        traces = []

        logger.info(f"Processing {filename}...")

        try:
            with open(path, "rb") as f:
                # Try to read header
                try:
                    header = f.read(16)
                    magic, survey_id, trace_count = struct.unpack("<8sII", header)
                    logger.debug(f"Header: Magic={magic}, SurveyID={survey_id}, TraceCount={trace_count}")
                except Exception as e:
                    logger.error(f"{filename} header could not be read: {e}")
                    return None

                # Read traces until EOF
                while True:
                    trace_bytes = f.read(13)
                    if len(trace_bytes) < 13:
                        break  # End of file
                    try:
                        well_id, depth, amplitude, quality = struct.unpack("<IffB", trace_bytes)
                        traces.append({
                            "well_id": well_id,
                            "depth": depth,
                            "amplitude": amplitude,
                            "quality": quality,
                            "survey_id": survey_id
                        })
                    except Exception:
                        continue  # Skip corrupted trace

            if traces:
                df = pd.DataFrame(traces)
                parquet_out = self.config.source_folder / filename.replace(".sgx", ".parquet")

                try:
                    table = pa.Table.from_pandas(df)
                    pq.write_table(table, parquet_out)
                    logger.info(f"{filename} -> {parquet_out.name} converted. Rows: {len(df)}")
                    return parquet_out
                except Exception as e:
                    logger.error(f"{filename} Parquet write failed: {e}")
                    return None
            else:
                logger.warning(f"No valid traces found in {filename}")
                return None

        except Exception as e:
            logger.error(f"Error processing {filename}: {e}")
            return None
