"""
The main ETL pipeline. Three stages I have done:
  1. Extract  - read raw files into pandas DataFrames
  2. Transform - clean, validate, and standardise the data
  3. Load      - insert into PostgreSQL using psycopg v3
"""

import io
import time
import json
import pandas as pd
import psycopg
from src.config import get_file_paths
from src.database import get_connection
from src.logger import get_logger

logger = get_logger("etl")

VALID_STATUSES = {"placed", "shipped", "cancelled", "refunded"}


# EXTRACT

def extract_customers(path: str) -> pd.DataFrame:
    """Read customers' data CSV into a DataFrame."""
    logger.info(f"Reading customers from: {path}")
    df = pd.read_csv(path)
    logger.info(f"  Rows read: {len(df)}")
    return df


def extract_orders(path: str) -> pd.DataFrame:
    """
    Read orders JSONL (one JSON object per line) into a DataFrame.
    JSONL is not standard CSV so it is read line by line.
    """
    logger.info(f"Reading orders from: {path}")
    records = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    df = pd.DataFrame(records)
    logger.info(f"  Rows read: {len(df)}")
    return df


def extract_order_items(path: str) -> pd.DataFrame:
    """Read order_items CSV into a DataFrame."""
    logger.info(f"Reading order_items from: {path}")
    df = pd.read_csv(path)
    logger.info(f"  Rows read: {len(df)}")
    return df


# TRANSFORM

def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and validate the customers DataFrame.

    Notes on decisions made in this function:
    - Normalise email to lowercase
    - Drop rows with invalid emails (an email must contain @)
    - Resolve duplicate emails by keeping the latest signup_date
    - Parse signup_date to a proper date type (UTC)
    - Fill missing country_code with None (It will be null in the database)
    """
    logger.info("Transforming customers...")
    start_count = len(df)

    # Normalise email to lowercase
    df["email"] = df["email"].str.lower().str.strip()

    # Drop rows where email doesn't contain @ — these are invalid
    invalid_email_mask = ~df["email"].str.contains("@", na=False)
    invalid_emails = df[invalid_email_mask]
    if len(invalid_emails) > 0:
        logger.warning(
            f"  Dropping {len(invalid_emails)} row(s) with invalid email: "
            + str(invalid_emails["email"].tolist())
        )
    df = df[~invalid_email_mask].copy()

    # Parse signup_date
    df["signup_date"] = pd.to_datetime(df["signup_date"]).dt.date

    # Resolve duplicate emails — keep the row with the latest signup_date.
    # Decision: the most recent record is most likely to have up-to-date information.
    df = df.sort_values("signup_date").drop_duplicates(subset=["email"], keep="last")
    duplicates_removed = start_count - len(invalid_emails) - len(df)
    if duplicates_removed > 0:
        logger.warning(f"  Removed {duplicates_removed} duplicate email(s), kept latest signup.")

    # Replace empty/NaN country_code with None so it stores as NULL
    df["country_code"] = df["country_code"].where(df["country_code"].notna(), None)

    # Ensure is_active is a proper boolean
    df["is_active"] = df["is_active"].astype(bool)

    logger.info(f"  Customers after cleaning: {len(df)} (started with {start_count})")
    return df.reset_index(drop=True)


def transform_orders(df: pd.DataFrame, valid_customer_ids: set) -> pd.DataFrame:
    """
    Clean and validate the orders DataFrame.

    Steps:
    - Parse order_ts to UTC (handles multiple timestamp formats)
    - Drop rows with invalid status values (Filter out records with unrecognised status values. 
      Logging these to a 'quarantine' list so that someone from the relevant team can investigate
      why the source system is sending through unrecognised values.)
    - Drop rows referencing customer_ids not in the customers table
    - Cast total_amount to float
    """
    logger.info("Transforming orders...")
    start_count = len(df)

    # Standardising timestamps: 'mixed' lets Pandas handle different incoming date formats.
    # utc=True converts everything to UTC regardless of the original timezone
    df["order_ts"] = pd.to_datetime(df["order_ts"], utc=True, format="mixed")

    # Filter out invalid status values
    invalid_status_mask = ~df["status"].isin(VALID_STATUSES)
    invalid_statuses = df[invalid_status_mask]
    if len(invalid_statuses) > 0:
        logger.warning(
            f"  Quarantining {len(invalid_statuses)} order(s) with invalid status: "
            + str(invalid_statuses[["order_id", "status"]].values.tolist())
        )
    df = df[~invalid_status_mask].copy()

    # Drop orders that reference a customer_id not in our customers table.
    # Decision: drop rather than default, because can't report on an unknown customer.
    orphan_mask = ~df["customer_id"].isin(valid_customer_ids)
    orphans = df[orphan_mask]
    if len(orphans) > 0:
        logger.warning(
            f"  Dropping {len(orphans)} order(s) referencing unknown customer_ids: "
            + str(orphans["customer_id"].tolist())
        )
    df = df[~orphan_mask].copy()

    # Cast total_amount to float (numeric in the database)
    df["total_amount"] = df["total_amount"].astype(float)

    logger.info(f"  Orders after cleaning: {len(df)} (started with {start_count})")
    return df.reset_index(drop=True)


def transform_order_items(df: pd.DataFrame, valid_order_ids: set) -> pd.DataFrame:
    """
    Clean and validate the order_items DataFrame.

    Steps:
    - Business Logic Check: Drop rows with zero or negative quantity/price (quarantine these for the relevant team).
    - Drop rows referencing order_ids not in the orders table
    """
    logger.info("Transforming order_items...")
    start_count = len(df)

    # Drop rows where quantity <= 0 or unit_price <= 0
    # Decision: filter rather than fix — we don't know what the correct value should be
    bad_values_mask = (df["quantity"] <= 0) | (df["unit_price"] <= 0)
    bad_rows = df[bad_values_mask]
    if len(bad_rows) > 0:
        logger.warning(
            f"  Dropping {len(bad_rows)} order_item(s) with non-positive quantity or price: "
            + str(bad_rows[["order_id", "line_no", "quantity", "unit_price"]].values.tolist())
        )
    df = df[~bad_values_mask].copy()

    # Drop items referencing orders that weren't loaded
    orphan_mask = ~df["order_id"].isin(valid_order_ids)
    orphans = df[orphan_mask]
    if len(orphans) > 0:
        logger.warning(
            f"  Dropping {len(orphans)} order_item(s) referencing unknown order_ids: "
            + str(orphans["order_id"].tolist())
        )
    df = df[~orphan_mask].copy()

    df["unit_price"] = df["unit_price"].astype(float)

    logger.info(f"  Order items after cleaning: {len(df)} (started with {start_count})")
    return df.reset_index(drop=True)


# LOAD

def load_customers(conn, df: pd.DataFrame):
    """
    Load customers into PostgreSQL using client-side COPY.
    COPY is much faster than row-by-row inserts for bulk loading.
    We use ON CONFLICT DO NOTHING so reruns don't crash on duplicates.
    """
    logger.info(f"Loading {len(df)} customers...")

    # Using an in-memory buffer (StringIO) for a high-performance bulk upload.
    buffer = io.StringIO()
    df[["customer_id", "email", "full_name", "signup_date", "country_code", "is_active"]].to_csv(
        buffer, index=False, header=False
    )

    # Rewind the buffer to the start so the DB driver can read it from the beginning.
    buffer.seek(0)

    with conn.cursor() as cur:
        # Load into a temporary staging table first
        cur.execute("""
            CREATE TEMP TABLE customers_stage (LIKE customers INCLUDING ALL) ON COMMIT DROP;
        """)
        with cur.copy("COPY customers_stage (customer_id, email, full_name, signup_date, country_code, is_active) FROM STDIN WITH CSV") as copy:
            copy.write(buffer.read())

        # Merge from staging into the real table, skipping conflicts
        cur.execute("""
            INSERT INTO customers SELECT * FROM customers_stage
            ON CONFLICT (customer_id) DO NOTHING;
        """)
        logger.info(f"  Customers loaded successfully.")


def load_orders(conn, df: pd.DataFrame):
    """Load orders into PostgreSQL using client-side COPY via a staging table."""
    logger.info(f"Loading {len(df)} orders...")

    buffer = io.StringIO()
    df[["order_id", "customer_id", "order_ts", "status", "total_amount", "currency"]].to_csv(
        buffer, index=False, header=False
    )
    buffer.seek(0)

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TEMP TABLE orders_stage (LIKE orders INCLUDING ALL) ON COMMIT DROP;
        """)
        with cur.copy("COPY orders_stage (order_id, customer_id, order_ts, status, total_amount, currency) FROM STDIN WITH CSV") as copy:
            copy.write(buffer.read())

        cur.execute("""
            INSERT INTO orders SELECT * FROM orders_stage
            ON CONFLICT (order_id) DO NOTHING;
        """)
        logger.info(f"  Orders loaded successfully.")


def load_order_items(conn, df: pd.DataFrame):
    """Load order_items into PostgreSQL using client-side COPY via a staging table."""
    logger.info(f"Loading {len(df)} order_items...")

    buffer = io.StringIO()
    df[["order_id", "line_no", "sku", "quantity", "unit_price", "category"]].to_csv(
        buffer, index=False, header=False
    )
    buffer.seek(0)

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TEMP TABLE order_items_stage (LIKE order_items INCLUDING ALL) ON COMMIT DROP;
        """)
        with cur.copy("COPY order_items_stage (order_id, line_no, sku, quantity, unit_price, category) FROM STDIN WITH CSV") as copy:
            copy.write(buffer.read())

        cur.execute("""
            INSERT INTO order_items SELECT * FROM order_items_stage
            ON CONFLICT (order_id, line_no) DO NOTHING;
        """)
        logger.info(f"  Order items loaded successfully.")

# VIEWS

def create_views(conn):
    """
    Create all analytics and data quality views.
    Views are dropped and recreated each run so they stay up to date.
    """
    logger.info("Creating SQL views...")

    views_sql = """
        
        -- ANALYTICS VIEWS

        -- 1. Daily metrics: orders count, total revenue, average order value
        CREATE OR REPLACE VIEW vw_daily_metrics AS
        SELECT
            order_ts::DATE                          AS date,
            COUNT(*)                                AS orders_count,
            SUM(total_amount)                       AS total_revenue,
            ROUND(AVG(total_amount), 2)             AS average_order_value
        FROM orders
        GROUP BY order_ts::DATE
        ORDER BY date;

        -- 2. Top 10 customers by lifetime spend
        CREATE OR REPLACE VIEW vw_top_customers AS
        SELECT
            c.customer_id,
            c.full_name,
            c.email,
            SUM(o.total_amount)                     AS lifetime_spend,
            COUNT(o.order_id)                       AS total_orders,
            RANK() OVER (ORDER BY SUM(o.total_amount) DESC) AS spend_rank
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.full_name, c.email
        ORDER BY lifetime_spend DESC
        LIMIT 10;

        -- 3. Top 10 SKUs by revenue and units sold
        CREATE OR REPLACE VIEW vw_top_skus AS
        SELECT
            sku,
            category,
            SUM(quantity)                           AS total_units_sold,
            SUM(quantity * unit_price)              AS total_revenue,
            RANK() OVER (ORDER BY SUM(quantity * unit_price) DESC) AS revenue_rank
        FROM order_items
        GROUP BY sku, category
        ORDER BY total_revenue DESC
        LIMIT 10;

        -- DATA QUALITY VIEWS

        -- 4. Duplicate customers by lowercase email (identifies conflicts in source data)
        CREATE OR REPLACE VIEW vw_dq_duplicate_emails AS
        SELECT
            email,
            COUNT(*)                                AS duplicate_count,
            STRING_AGG(customer_id::TEXT, ', ')     AS customer_ids,
            MIN(signup_date)                        AS earliest_signup
        FROM customers
        GROUP BY email
        HAVING COUNT(*) > 1;

        -- 5. Orders referencing customer_ids that don't exist in the customers table
        CREATE OR REPLACE VIEW vw_dq_orphan_orders AS
        SELECT
            o.order_id,
            o.customer_id,
            o.order_ts,
            o.status,
            o.total_amount
        FROM orders o
        LEFT JOIN customers c ON o.customer_id = c.customer_id
        WHERE c.customer_id IS NULL;
    """

    with conn.cursor() as cur:
        cur.execute(views_sql)
    conn.commit()
    logger.info("Views created successfully.")

# MAIN PIPELINE RUNNER

def run_pipeline():
    """
    Runs the full ETL pipeline end-to-end:
      Extract → Transform → Load → Create Views
    """
    paths = get_file_paths()
    start = time.time()
    logger.info("=" * 60)
    logger.info("Pipeline started")
    logger.info("=" * 60)

    # --- EXTRACT ---
    raw_customers = extract_customers(paths["customers"])
    raw_orders = extract_orders(paths["orders"])
    raw_order_items = extract_order_items(paths["order_items"])

    # --- TRANSFORM ---
    clean_customers = transform_customers(raw_customers)
    valid_customer_ids = set(clean_customers["customer_id"].tolist())

    clean_orders = transform_orders(raw_orders, valid_customer_ids)
    valid_order_ids = set(clean_orders["order_id"].tolist())

    clean_order_items = transform_order_items(raw_order_items, valid_order_ids)

    # --- LOAD ---
    with get_connection() as conn:
        load_customers(conn, clean_customers)
        load_orders(conn, clean_orders)
        load_order_items(conn, clean_order_items)
        conn.commit()
        create_views(conn)

    elapsed = round(time.time() - start, 2)
    logger.info("=" * 60)
    logger.info(f"Pipeline completed in {elapsed}s")
    logger.info("=" * 60)
