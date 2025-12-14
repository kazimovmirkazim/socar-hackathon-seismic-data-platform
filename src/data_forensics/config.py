from dataclasses import dataclass
from pathlib import Path

@dataclass
class Config:
    source_folder: Path = Path("./track_1_forensics")
    data_lake_raw: Path = Path("./data_lake/raw/")
    corrupted_file_name: str = "archive_batch_seismic_readings.parquet"
    backup_suffix: str = "_CORRUPTED_BACKUP.parquet"
    clean_suffix: str = "_CLEAN.parquet"
    log_level: str = "INFO"

    def __post_init__(self):
        # Ensure paths are Path objects
        self.source_folder = Path(self.source_folder)
        self.data_lake_raw = Path(self.data_lake_raw)
        self.data_lake_raw.mkdir(parents=True, exist_ok=True)
