# LexisNexis Data Engineer Assessment
### Orders Data Pipeline — Python + PostgreSQL

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
