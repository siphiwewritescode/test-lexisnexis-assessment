"""
Unit tests for the three transform functions in src/etl.py.

Each test builds a small pandas DataFrame, runs the transform,
and checks the output. No database or file I/O is needed.
"""

import datetime
import pandas as pd
import pytest

from src.etl import transform_customers, transform_orders, transform_order_items, VALID_STATUSES


# ---------------------------------------------------------------------------
# Helper: build a simple customers DataFrame
# ---------------------------------------------------------------------------
def _customers_df(rows):
    """Shortcut to build a customers DataFrame from a list of dicts."""
    cols = ["customer_id", "email", "full_name", "signup_date", "country_code", "is_active"]
    return pd.DataFrame(rows, columns=cols)


# ===========================================================================
# transform_customers
# ===========================================================================

class TestTransformCustomers:

    def test_email_is_lowercased(self):
        df = _customers_df([
            [1, "Alice@Example.COM", "Alice", "2024-01-01", "US", True],
        ])
        result, _q = transform_customers(df)
        assert result["email"].iloc[0] == "alice@example.com"

    def test_email_whitespace_is_stripped(self):
        df = _customers_df([
            [1, "  bob@example.com  ", "Bob", "2024-01-01", "UK", True],
        ])
        result, _q = transform_customers(df)
        assert result["email"].iloc[0] == "bob@example.com"

    def test_invalid_email_without_tld_is_dropped(self):
        df = _customers_df([
            [1, "good@example.com", "Good", "2024-01-01", "US", True],
            [2, "bad@nodot", "Bad", "2024-01-01", "US", True],
        ])
        result, quarantine = transform_customers(df)
        assert len(result) == 1
        assert result["email"].iloc[0] == "good@example.com"
        assert len(quarantine) == 1
        assert quarantine[0]["reason"] == "invalid_email"

    def test_invalid_email_missing_at_sign_is_dropped(self):
        df = _customers_df([
            [1, "notanemail", "Nope", "2024-01-01", "US", True],
        ])
        result, quarantine = transform_customers(df)
        assert len(result) == 0
        assert len(quarantine) == 1
        assert quarantine[0]["reason"] == "invalid_email"

    def test_duplicate_email_keeps_latest_signup(self):
        df = _customers_df([
            [1, "dup@example.com", "Old Record", "2024-01-01", "US", True],
            [2, "dup@example.com", "New Record", "2024-06-15", "UK", False],
        ])
        result, quarantine = transform_customers(df)
        assert len(result) == 1
        # The row with the later signup_date should survive
        assert result["customer_id"].iloc[0] == 2
        assert result["full_name"].iloc[0] == "New Record"
        # The old record should be quarantined as a duplicate
        dup_records = [r for r in quarantine if r["reason"] == "duplicate_email"]
        assert len(dup_records) == 1

    def test_missing_country_code_becomes_none(self):
        df = _customers_df([
            [1, "user@example.com", "User", "2024-01-01", None, True],
        ])
        result, _q = transform_customers(df)
        assert result["country_code"].iloc[0] is None

    def test_nan_country_code_becomes_null(self):
        df = _customers_df([
            [1, "user@example.com", "User", "2024-01-01", float("nan"), True],
        ])
        result, _q = transform_customers(df)
        # NaN in a float column stays as NaN (pandas equivalent of NULL)
        assert pd.isna(result["country_code"].iloc[0])

    def test_is_active_cast_to_bool(self):
        df = _customers_df([
            [1, "a@example.com", "A", "2024-01-01", "US", 1],
            [2, "b@example.com", "B", "2024-01-01", "US", 0],
        ])
        result, _q = transform_customers(df)
        assert result["is_active"].iloc[0] is True or result["is_active"].iloc[0] == True
        assert result["is_active"].iloc[1] is False or result["is_active"].iloc[1] == False

    def test_signup_date_is_parsed(self):
        df = _customers_df([
            [1, "a@example.com", "A", "2024-03-15", "US", True],
        ])
        result, _q = transform_customers(df)
        assert result["signup_date"].iloc[0] == datetime.date(2024, 3, 15)

    def test_index_is_reset(self):
        df = _customers_df([
            [1, "a@example.com", "A", "2024-01-01", "US", True],
            [2, "b@example.com", "B", "2024-01-01", "US", True],
        ])
        result, _q = transform_customers(df)
        assert list(result.index) == [0, 1]


# ===========================================================================
# transform_orders
# ===========================================================================

def _orders_df(rows):
    """Shortcut to build an orders DataFrame from a list of lists."""
    cols = ["order_id", "customer_id", "order_ts", "status", "total_amount", "currency"]
    return pd.DataFrame(rows, columns=cols)


class TestTransformOrders:

    def test_timestamp_parsed_to_utc(self):
        df = _orders_df([
            [1, 10, "2024-03-15T10:30:00Z", "placed", 99.99, "USD"],
        ])
        result, _q = transform_orders(df, valid_customer_ids={10})
        ts = result["order_ts"].iloc[0]
        # Should be timezone-aware UTC
        assert str(ts.tzinfo) == "UTC"

    def test_invalid_status_is_dropped(self):
        df = _orders_df([
            [1, 10, "2024-01-01T00:00:00Z", "placed", 10.0, "USD"],
            [2, 10, "2024-01-01T00:00:00Z", "unknown_status", 20.0, "USD"],
            [3, 10, "2024-01-01T00:00:00Z", "shipped", 30.0, "USD"],
        ])
        result, quarantine = transform_orders(df, valid_customer_ids={10})
        assert len(result) == 2
        assert set(result["status"]) == {"placed", "shipped"}
        assert len(quarantine) == 1
        assert quarantine[0]["reason"] == "invalid_status"

    def test_all_valid_statuses_are_kept(self):
        valid = ["placed", "shipped", "cancelled", "refunded"]
        rows = [
            [i, 10, "2024-01-01T00:00:00Z", status, 10.0, "USD"]
            for i, status in enumerate(valid)
        ]
        df = _orders_df(rows)
        result, quarantine = transform_orders(df, valid_customer_ids={10})
        assert len(result) == 4
        assert len(quarantine) == 0

    def test_orphan_customer_id_is_dropped(self):
        df = _orders_df([
            [1, 10, "2024-01-01T00:00:00Z", "placed", 50.0, "USD"],
            [2, 999, "2024-01-01T00:00:00Z", "placed", 75.0, "USD"],
        ])
        result, quarantine = transform_orders(df, valid_customer_ids={10})
        assert len(result) == 1
        assert result["customer_id"].iloc[0] == 10
        orphan_records = [r for r in quarantine if r["reason"] == "orphan_customer"]
        assert len(orphan_records) == 1

    def test_total_amount_cast_to_float(self):
        df = _orders_df([
            [1, 10, "2024-01-01T00:00:00Z", "placed", "123", "USD"],
        ])
        result, _q = transform_orders(df, valid_customer_ids={10})
        assert isinstance(result["total_amount"].iloc[0], float)
        assert result["total_amount"].iloc[0] == 123.0

    def test_index_is_reset(self):
        df = _orders_df([
            [1, 10, "2024-01-01T00:00:00Z", "placed", 10.0, "USD"],
            [2, 10, "2024-01-01T00:00:00Z", "shipped", 20.0, "USD"],
        ])
        result, _q = transform_orders(df, valid_customer_ids={10})
        assert list(result.index) == [0, 1]


# ===========================================================================
# transform_order_items
# ===========================================================================

def _order_items_df(rows):
    """Shortcut to build an order_items DataFrame from a list of lists."""
    cols = ["order_id", "line_no", "sku", "quantity", "unit_price", "category"]
    return pd.DataFrame(rows, columns=cols)


class TestTransformOrderItems:

    def test_zero_quantity_is_dropped(self):
        df = _order_items_df([
            [1, 1, "SKU-A", 0, 10.0, "Books"],
            [1, 2, "SKU-B", 3, 10.0, "Books"],
        ])
        result, quarantine = transform_order_items(df, valid_order_ids={1})
        assert len(result) == 1
        assert result["sku"].iloc[0] == "SKU-B"
        assert len(quarantine) == 1
        assert quarantine[0]["reason"] == "invalid_quantity_or_price"

    def test_negative_quantity_is_dropped(self):
        df = _order_items_df([
            [1, 1, "SKU-A", -2, 10.0, "Books"],
        ])
        result, quarantine = transform_order_items(df, valid_order_ids={1})
        assert len(result) == 0
        assert len(quarantine) == 1
        assert quarantine[0]["reason"] == "invalid_quantity_or_price"

    def test_zero_price_is_dropped(self):
        df = _order_items_df([
            [1, 1, "SKU-A", 5, 0.0, "Books"],
        ])
        result, quarantine = transform_order_items(df, valid_order_ids={1})
        assert len(result) == 0
        assert len(quarantine) == 1

    def test_negative_price_is_dropped(self):
        df = _order_items_df([
            [1, 1, "SKU-A", 5, -9.99, "Books"],
        ])
        result, quarantine = transform_order_items(df, valid_order_ids={1})
        assert len(result) == 0
        assert len(quarantine) == 1

    def test_positive_quantity_and_price_are_kept(self):
        df = _order_items_df([
            [1, 1, "SKU-A", 2, 15.50, "Books"],
        ])
        result, quarantine = transform_order_items(df, valid_order_ids={1})
        assert len(result) == 1
        assert len(quarantine) == 0

    def test_orphan_order_id_is_dropped(self):
        df = _order_items_df([
            [1, 1, "SKU-A", 2, 10.0, "Books"],
            [999, 1, "SKU-B", 1, 5.0, "Books"],
        ])
        result, quarantine = transform_order_items(df, valid_order_ids={1})
        assert len(result) == 1
        assert result["order_id"].iloc[0] == 1
        orphan_records = [r for r in quarantine if r["reason"] == "orphan_order"]
        assert len(orphan_records) == 1

    def test_unit_price_cast_to_float(self):
        df = _order_items_df([
            [1, 1, "SKU-A", 2, 10, "Books"],
        ])
        result, _q = transform_order_items(df, valid_order_ids={1})
        assert isinstance(result["unit_price"].iloc[0], float)

    def test_index_is_reset(self):
        df = _order_items_df([
            [1, 1, "SKU-A", 1, 10.0, "Books"],
            [1, 2, "SKU-B", 2, 20.0, "Books"],
        ])
        result, _q = transform_order_items(df, valid_order_ids={1})
        assert list(result.index) == [0, 1]
