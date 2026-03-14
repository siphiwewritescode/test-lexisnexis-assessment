"""
Single entry point for the pipeline.

python main.py - Initialises the schema and runs the full ETL pipeline
"""

from src.database import init_schema
from src.etl import run_pipeline
from src.logger import get_logger

logger = get_logger("main")


def main():
    logger.info("Running: init schema")
    init_schema()
    logger.info("Running: ETL pipeline")
    run_pipeline()

if __name__ == "__main__":
    main()
