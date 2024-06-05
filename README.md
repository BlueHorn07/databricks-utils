# databricks-utils

🚀 A Collection of Useful Databricks Code for Your Projects 💡

# Setup

Copy `.env.example` as `.env` file, and fill-out the required env variables.

```dotenv
DATABRICKS_TOKEN=dapixxxx
DATABRICKS_HOST_URL=https://xxxx.databricks.com
```

# Commands

## Get Job Run Report

```bash
python3 -m src.get_job_run_report \
    --job-id xxxx \
    --num-job-runs 10
```

## Compare Two Job Run Report

```bash
python3 -m src.compare_two_job_run_report \
    --job-id-1 xxxx \
    --job-id-2 xxxx \
    --num-job-runs 10
```

Output:

```text
===== REPORT =====
"this-is-sample-job-name         " (1234567890 ): (Avg, Max, Min) = (200, 330, 180) Seconds
"this-is-longer-job-nameeeeeeeeee" (12345678900): (Avg, Max, Min) = (300, 310, 280) Seconds
==================
```

## Search Jobs by Notebook Path

```bash
python3 -m src.search_jobs_by_notebook_path \
    --path xxxx
```
