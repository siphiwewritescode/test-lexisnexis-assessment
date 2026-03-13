"""
Single entry point for the pipeline.

Commands to run from the console:
  python main.py init   - Create the database schema (tables)
  python main.py run    - Run the full ETL pipeline
"""

import sys
from src.database import init_schema
from src.etl import run_pipeline
from src.logger import get_logger

logger = get_logger("main")


def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py [init|run]")
        sys.exit(1)

    command = sys.argv[1].lower()

    if command == "init":
        logger.info("Running: init schema")
        init_schema()

    elif command == "run":
        logger.info("Running: ETL pipeline")
        run_pipeline()

    else:
        print(f"Unknown command: '{command}'. Use 'init' or 'run'.")
        sys.exit(1)


if __name__ == "__main__":
    main()
