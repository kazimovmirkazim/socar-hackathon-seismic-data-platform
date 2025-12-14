"""
Command-line entry point, so the package can be executed with:
    python -m data_forensics
"""
from .pipeline import ForensicsPipeline

def main() -> None:
    ForensicsPipeline().run()

if __name__ == "__main__":
    main()
