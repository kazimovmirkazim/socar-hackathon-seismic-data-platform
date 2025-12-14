import logging
import sys

def setup_logger(name: str = "data_forensics", level: str = "INFO"):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger

def dump_hex(data: bytes, length: int = 100, label: str = "Data"):
    """Logs hex dump of bytes."""
    logger = logging.getLogger("data_forensics")
    logger.info(f"--- {label} (first {length} bytes) ---")
    logger.info(f"Hex: {data[:length].hex()}")
    logger.info(f"Text: {repr(data[:length])}")
