# SOLUTION.md — Design Decisions & Trade-offs

---

## Schema decisions

### customers
- `country_code` is **nullable**. The source data includes rows with a missing country code (customer_id 3). I chose to keep these records rather than discard them, because the customer data is still valid — we just don't know their country. Discarding valid customers would cause downstream referential issues with orders.
- Email is stored **lowercase** and has a **unique constraint**. This is enforced at the database level as a second line of defence, after normalization happens in Python during ETL.

### orders
- `status` uses a **CHECK constraint** to allow only `placed`, `shipped`, `cancelled`, `refunded`. This enforces the rule at the database level even if the ETL layer doesn't catch something.
- The foreign key on `customer_id` ensures referential integrity. Orders referencing unknown customers are dropped during ETL (see below).

### order_items
- Composite primary key on `(order_id, line_no)` — this is the natural identifier for a line item.
- CHECK constraints enforce `quantity > 0` and `unit_price > 0` at the database level.

---

## ETL decisions

### Duplicate emails (customers)
**Decision: keep the latest signup_date, drop the rest.**

Reasoning: the most recent record is most likely to have up-to-date information. I sort by signup_date ascending and use `drop_duplicates(keep="last")`. This is deterministic and easy to explain.

### Invalid emails (no @ sign)
**Decision: drop and log.**

An email with no `@` cannot be normalized or matched against anything — it's unusable data. I log the dropped rows so they can be investigated at the source.

### Invalid order status values (e.g. "processing")
**Decision: quarantine (drop) and log.**

The spec defines exactly four valid statuses. Mapping an unknown status to a default (e.g. "placed") would be guessing at intent, which is dangerous in a real pipeline. I drop these rows and log them clearly so they can be fixed upstream.

### Orders referencing unknown customers
**Decision: drop and log.**

A referential integrity error can't be resolved without more information. I don't want to insert a placeholder customer. Dropping and logging is the safest approach — the source system should be the one to fix this.

### Non-positive quantities and unit prices (order_items)
**Decision: drop and log.**

A quantity of 0 or a price of 0.00 is almost always a data entry error. I don't know what the correct value should be, so I can't fix it. I drop the rows and log them.

---

## Loading approach

I used **client-side COPY via psycopg v3** as the primary loading method. The workflow is:
1. Write the cleaned DataFrame to an in-memory CSV buffer (no temp file on disk)
2. COPY into a staging table
3. INSERT from staging into the real table using `ON CONFLICT DO NOTHING`

This approach is fast for bulk loads and idempotent — running the pipeline twice won't create duplicate records.

**Batched inserts** would be an acceptable fallback and are easier to read, but COPY is significantly faster at scale. Since the spec prefers COPY, I implemented it as the primary method.

---

## Configuration

I chose a `.env` file with `python-dotenv` because:
- It's simple and widely understood
- Credentials stay out of source code
- It works the same way on local machines and in CI environments
- No extra dependencies beyond `python-dotenv`

---

## Analytics views

All views are created with `CREATE OR REPLACE VIEW` so they stay current without needing to drop them manually.

- **vw_daily_metrics** — uses `order_ts::DATE` to group by day, handles timezone-aware timestamps correctly since order_ts is stored in UTC.
- **vw_top_customers** — uses a window function (`RANK()`) to rank by spend. Ties would get the same rank.
- **vw_top_skus** — revenue calculated as `quantity * unit_price` per line item, aggregated per SKU.

---

## What I would add with more time

- A quarantine table to persist rejected rows rather than just logging them then load to the relevant views
- Unit tests for each transform function
- Exception handling for database.py
- Docker Compose setup so reviewers don't need to install PostgreSQL manually
- The optional agentic REPORT.md generation