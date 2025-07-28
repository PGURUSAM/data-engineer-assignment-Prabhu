

### ETL Pipeline Project

A production-ready ETL (Extract, Transform, Load) pipeline designed for handling time-series energy consumption data with validation, feature engineering, and email alerting.

---
## Project Structure
```markdown



etl-pipeline-project/
│
├── etl\_pipeline/
│   ├── **init**.py
│   ├── utils.py                # Utility functions (metrics, time conversion, retries)
│   ├── validation.py           # Data validation (schema, IDs, missing values, energy column, quality score)
│   ├── feature\_engineering.py  # Feature engineering & aggregation logic
│   ├── alert.py                # Email alerting logic
│   ├── extract.py              # Data extraction logic
│   ├── transform.py            # Data transformation & cleaning logic
│   ├── load.py                 # Data loading logic
│   ├── scheduler.py            # Pipeline scheduling
│   ├── pipeline.py             # ETL orchestration module
│
├── main.py                     # Entry point for running the pipeline
├── requirements.txt            # Python dependencies
├── .env                        # Environment variables (email, DB paths, etc.)
├── README.md                   # Documentation
└── tests/
├── test\_sample.py
```

---

## Key Features

- **Data Extraction:** Pull raw energy consumption data from source.
- **Data Validation:**
  - Schema checks
  - ID format validation
  - Missing value detection
  - Energy consumption format validation
  - Data quality scoring
- **Transformation:**
  - Convert local time to UTC
  - Data cleaning & type conversion
  - Feature engineering and aggregation
- **Email Alerts:** Failure notifications through SMTP.
- **Scheduling:** Automates periodic runs using `schedule`.
- **Logging & Monitoring:** Logs events and system metrics (CPU/memory).

---

## Tech Stack

- **Language:** Python 3.x
- **Libraries:**  
  `pandas`, `numpy`, `pyarrow`,  
  `pytz`, `schedule`, `psutil`,  
  `smtplib` (for email alerts)
- **Deployment:** Designed for local or cloud execution.

---

## Pre-requisites

- Python 3.8+
- Install dependencies:

```bash
pip install -r requirements.txt
````

* Configure environment variables in `.env`:

```ini
ALERT_EMAIL=youremail@example.com
SMTP_SERVER=smtp.example.com
SMTP_USER=your_smtp_user
SMTP_PASS=your_smtp_password
```

---

## Steps to Run the Pipeline

1. **Clone the repository**

```bash
git clone <repo-url>
cd etl-pipeline-project
```

2. **Install dependencies**

```bash
pip install -r requirements.txt
```

3. **Set up environment variables**
   Update `.env` file with correct email and server configurations.

4. **Run the pipeline manually**

```bash
python main.py
```

5. **Schedule automatic runs**
   The scheduler in `etl_pipeline/scheduler.py` is configured to run periodically.
   You can trigger:

```bash
python -m etl_pipeline.scheduler
```

---

## Testing

Run unit tests using pytest:

```bash
pytest tests/
```

---

## Error Handling & Alerts

* All failures during extraction, transformation, or loading trigger an alert email (if configured).
* Logs are written to console for real-time monitoring.

---

## Extending the Pipeline

* Add new data sources in `extract.py`
* Add custom feature engineering in `feature_engineering.py`
* Extend alerts in `alert.py`

---

## Versioning

**Current Version:** 1.0.0

---

## Running the ETL Pipeline

Run the ETL pipeline:

```bash
python main.py
```

### Sample output:

```
2025-07-28 07:25:53,417 [INFO] Starting ETL pipeline in once mode
2025-07-28 07:25:53,417 [INFO] Running ETL version 1.0.0
2025-07-28 07:25:54,437 [INFO] System usage: CPU 2.5%, Memory 82.5%
2025-07-28 07:25:54,437 [INFO] [extract] Starting extraction from /path/to/home_assignment_raw_data.parquet
2025-07-28 07:25:54,437 [INFO] Attempt 1: Reading parquet /path/to/home_assignment_raw_data.parquet
2025-07-28 07:25:54,492 [INFO] [extract] Parquet file read successfully
2025-07-28 07:25:54,492 [INFO] Schema validation passed.
2025-07-28 07:25:54,492 [INFO] [extract] Extraction completed in 0.06s
2025-07-28 07:25:54,492 [INFO] [transform] Starting transformation
2025-07-28 07:25:54,498 [INFO] [transform] Data types converted
2025-07-28 07:25:54,677 [INFO] [transform] Date column converted to UTC
2025-07-28 07:25:54,677 [INFO] [transform] ID formats validated
2025-07-28 07:25:54,682 [INFO] [transform] Energy consumption column validated
2025-07-28 07:25:54,684 [INFO] No missing values detected.
2025-07-28 07:25:54,685 [INFO] Data Quality Score: 100.00/100
2025-07-28 07:25:54,766 [INFO] [transform] Transformation completed in 0.27s
2025-07-28 07:25:54,766 [INFO] [load] Starting load to /path/to/output.parquet
2025-07-28 07:25:54,795 [INFO] [load] Output written to /path/to/output.parquet
2025-07-28 07:25:54,795 [INFO] ETL job completed successfully.
```

---

## Running Tests

Run unit tests from the project root:

```bash
python -m pytest
```

### Sample test output:

```
======================= test session starts ========================
platform win32 -- Python 3.9.13, pytest-8.4.1, pluggy-1.6.0
rootdir: /path/to/etl-pipeline-project
plugins: typeguard-4.4.4
collected 3 items

tests/test_sample.py ...                                      [100%]

======================== 3 passed in 0.62s =========================
```

