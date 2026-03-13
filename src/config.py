"""
This file reads the .env file and makes sure that the settings are available
to the rest of the code. Database and file path settings need to be ready before we connect to anything
"""

import os
from dotenv import load_dotenv

# Load values from .env file if it exists
load_dotenv()


def get_db_dsn() -> str:
    """Build and return the PostgreSQL connection string."""
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "lexisnexis_assessment")
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "root")
    return f"host={host} port={port} dbname={name} user={user} password={password}"


def get_file_paths() -> dict:
    """Return paths to the three data files from the data folder that will be read, cleaned and inserted into the database."""
    return {
        "customers": os.getenv("CUSTOMERS_FILE", "data/customers.csv"),
        "orders": os.getenv("ORDERS_FILE", "data/orders.jsonl"),
        "order_items": os.getenv("ORDER_ITEMS_FILE", "data/order_items.csv"),
    }