# LexisNexis Data Engineer Assessment
### Orders Data Pipeline — Python + PostgreSQL

[Explainer Video](https://youtu.be/Zvvt4kD0nRI)

A Python ETL pipeline that reads raw orders data (CSV + JSONL), cleans and validates it, loads it into PostgreSQL, and creates SQL views for analytics and data quality monitoring.

---

## Script architecture

```
┌─────────────┐
│   main.py   │  Entry point — runs init + ETL pipeline
└──────┬──────┘
       │ imports
       ├──────────────────────────────────────┐
       ▼                                      ▼
┌─────────────────┐                  ┌─────────────────┐
│  src/database.py│                  │   src/etl.py    │
│                 │                  │                 │
│ init_schema()   │                  │ run_pipeline()  │
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
| `main.py` | Entry point; runs schema init + ETL pipeline |
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

**1. Create and activate a virtual environment**
```bash
python -m venv venv

# Mac/Linux
source venv/bin/activate

# Windows
venv\Scripts\activate
```

**2. Install dependencies**
```bash
pip install -r requirements.txt
```

**3. Configure your environment**
```bash
# Mac/Linux
cp .env.example .env

# Windows
copy .env.example .env
```
Then edit `.env` with your database credentials.

**4. Create a PostgreSQL database**
```bash
# Create the database in pgAdmin or via psql:
# CREATE DATABASE "test-lexisnexis-assessment";
```

---

## Running the pipeline

**Run the pipeline**
```bash
python main.py
```
This will create the schema (if it doesn't exist) and run the full ETL pipeline.

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
├── main.py                # Entry point
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
