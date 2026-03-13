# LexisNexis Data Engineer Assessment
### Orders Data Pipeline — Python + PostgreSQL

A Python ETL pipeline that reads raw orders data (CSV + JSONL), cleans and validates it, loads it into PostgreSQL, and creates SQL views for analytics and data quality monitoring.

---

## Script architecture

```
┌─────────────┐
│   main.py   │  Entry point — routes `init` or `run` commands
└──────┬──────┘
       │ imports
       ├──────────────────────────────────────┐
       ▼                                      ▼
┌─────────────────┐                  ┌─────────────────┐
│  src/database.py│                  │   src/etl.py    │
│                 │                  │                 │
│ init_schema()   │◄─── `init`       │ run_pipeline()  │◄─── `run`
│ get_connection()│                  │                 │
└─────────────────┘                  └────────┬────────┘
                                              │ imports
                              ┌───────────────┼───────────────┐
                              ▼               ▼               ▼
                     ┌─────────────┐  ┌─────────────┐  ┌──────────────┐
                     │ src/config  │  │ src/database│  │  src/logger  │
                     │ .py         │  │ .py         │  │  .py         │
                     │             │  │             │  │              │
                     │ get_file_   │  │ get_        │  │ get_logger() │
                     │ paths()     │  │ connection()│  │              │
                     └──────┬──────┘  └──────┬──────┘  └──────────────┘
                            │                │
                            ▼                ▼
                     ┌────────────┐   ┌────────────┐
                     │  .env file │   │ PostgreSQL  │
                     └────────────┘   └────────────┘
```

**ETL flow inside `etl.py`:**
```
data/customers.csv   ──► extract ──► transform ──► load ──► customers table
data/orders.jsonl    ──► extract ──► transform ──► load ──► orders table
data/order_items.csv ──► extract ──► transform ──► load ──► order_items table
                                                       └──► create_views()
                                                             (5 SQL views)
```

| File | Role |
|---|---|
| `main.py` | CLI entry point; routes `init` / `run` |
| `src/database.py` | Schema creation, DB connection |
| `src/etl.py` | All extract, transform, load, and view logic |
| `src/config.py` | Reads `.env` for file paths & DB settings |
| `src/logger.py` | Shared logger used by all modules |

---

## What this does

A small, maintainable ETL pipeline that:
1. Reads raw customer, order, and order item data from files
2. Cleans and validates the data (handles duplicates, bad formats, invalid values)
3. Loads the clean data into PostgreSQL
4. Creates SQL views for analytics and data quality monitoring

---

## Requirements

- Python 3.10+
- PostgreSQL 14+
- pip

---

## Setup

**1. Clone the repo and install dependencies**
```bash
git clone <your-repo-url>
cd test-lexisnexis-assessment
pip install -r requirements.txt
```

**2. Create a PostgreSQL database**
```sql
CREATE DATABASE test-lexisnexis-assessment;
```

**3. Configure your environment**
```bash
cp .env.example .env
# Edit .env with your database credentials
```

---

## Running the pipeline

**Step 1 — Create the schema (tables)**
```bash
python main.py init
```

**Step 2 — Run the ETL pipeline**
```bash
python main.py run
```

That's it. Both steps are safe to run multiple times (idempotent).

---

## Project structure

```
├── data/                  # Raw input files
│   ├── customers.csv
│   ├── orders.jsonl
│   └── order_items.csv
├── src/
│   ├── config.py          # Loads .env settings
│   ├── database.py        # Connection + schema creation
│   ├── etl.py             # Extract, Transform, Load logic + views
│   └── logger.py          # Logging setup
├── main.py                # Entry point (init / run)
├── requirements.txt
├── .env.example
├── README.md
└── SOLUTION.md            # Design decisions and trade-offs
```

---

## What gets logged

Every step logs:
- Start and end of each stage
- Row counts before and after cleaning
- Any rows that were dropped or quarantined (and why)
- Total pipeline duration
