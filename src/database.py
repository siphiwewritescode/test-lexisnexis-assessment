"""
This file handles the database connection and schema creation.
The init() function creates all three tables if they don't already exist. 
The init() fuction will be called at the start of the pipeline (when we run the command on the console) to ensure 
the database is ready before we attempt to load any data.
"""

import psycopg
from src.config import get_db_dsn
from src.logger import get_logger

logger = get_logger("database")


def get_connection():
    """Open and return a psycopg v3 connection."""
    dsn = get_db_dsn()
    return psycopg.connect(dsn)


def init_schema():
    """
    Create the customers, orders, and order_items tables.
    Using 'IF NOT EXISTS' makes this idempotent, meaning we can safely retry 
    this function if the pipeline crashes without hitting 'Table Already Exists' errors.
    """
    logger.info("Initialising database schema...")

    schema_sql = """
        -- Customers table
        -- Emails are stored in lowercase and must be unique.
        -- country_code is allowed to be null because the source data has missing values
        -- and we want to keep those records rather than discard them.
        CREATE TABLE IF NOT EXISTS customers (
            customer_id  INTEGER PRIMARY KEY,
            email        TEXT NOT NULL,
            full_name    TEXT NOT NULL,
            signup_date  DATE NOT NULL,
            country_code CHAR(2),
            is_active    BOOLEAN NOT NULL DEFAULT true,
            CONSTRAINT customers_email_unique UNIQUE (email),
            CONSTRAINT customers_email_lowercase CHECK (email = LOWER(email))
        );

        -- Orders table
        -- status is constrained to the four allowed values.
        -- Any other value will be rejected at the database level as a second line of defence
        -- (we also filter in Python before loading).
        CREATE TABLE IF NOT EXISTS orders (
            order_id      BIGINT PRIMARY KEY,
            customer_id   INTEGER NOT NULL REFERENCES customers(customer_id),
            order_ts      TIMESTAMP WITH TIME ZONE NOT NULL,
            status        TEXT NOT NULL,
            total_amount  NUMERIC(12,2) NOT NULL,
            currency      CHAR(3) NOT NULL,
            CONSTRAINT orders_status_check CHECK (
                status IN ('placed', 'shipped', 'cancelled', 'refunded')
            )
        );

        -- Order items table
        -- Composite primary key on (order_id, line_no).
        -- quantity and unit_price must be positive — zero or negative values
        -- indicate bad source data and are filtered out during ETL.
        CREATE TABLE IF NOT EXISTS order_items (
            order_id    BIGINT NOT NULL REFERENCES orders(order_id),
            line_no     INTEGER NOT NULL,
            sku         TEXT NOT NULL,
            quantity    INTEGER NOT NULL,
            unit_price  NUMERIC(12,2) NOT NULL,
            category    TEXT NOT NULL,
            PRIMARY KEY (order_id, line_no),
            CONSTRAINT order_items_quantity_positive CHECK (quantity > 0),
            CONSTRAINT order_items_price_positive CHECK (unit_price > 0)
        );
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
        conn.commit()

    logger.info("Schema set up successfully.")
