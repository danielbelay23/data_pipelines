## ETL Pipeline: Twitter Data to BigQuery

### Overview
- **Extract**: Collects Twitter data using `twikit` and writes JSON files to `data/raw/`.
- **Transform**: Lightweight normalization and session logging; schema handled downstream.
- **Load**: Downloads raw JSON from GCS, materializes to SQLite for schema inference, and loads to BigQuery.

### Key Components
- `src/data_jobs/data_ingestion.py`: Twitter extraction, de-duplication, media extraction, session logging
- `src/data_jobs/db_manager.py`: GCS download, SQLite materialization, schema sync, BigQuery load
- `src/data_jobs/__init__.py`: Paths and GCP env variables
- `main.py`: Orchestrates end-to-end or step-specific runs
- `Dockerfile`: Build runtime with Python 3.9 and Google Cloud SDK
- `Jenkinsfile`: CI/CD pipeline to build, run, sync to GCS, and load to BigQuery

---

## Data Flow

### 1) Extract
- Authenticates with `twikit` using environment variables: `TWITTER_USERNAME`, `TWITTER_EMAIL`, `TWITTER_PASSWORD`, `TWITTER_TOTP_SECRET`.
- Writes artifacts to:
  - `data/raw/following.json`
  - `data/raw/tweets.json`
  - `data/processed/logging.json` (session logs: errors, attempts, counts)
- Rate limit aware; resumes incrementally by skipping already-seen IDs per day.

Run extract only:
```bash
python main.py --ingest-only
```

### 2) Transform (lightweight)
- Normalizes tweet records (e.g., media info) and logs sessions.
- No heavy transformations; schema evolution handled during load.

### 3) Load
- Reads envs from `src/data_jobs/__init__.py`:
  - `GCP_PROJECT_ID`
  - `GCS_BUCKET_NAME`
  - `BIGQUERY_DATASET_ID`
- Steps per table (`following`, `tweets`, `cookies`):
  1. Download `gs://${GCS_BUCKET_NAME}/data/raw/*.json` to a temp dir
  2. Create/update SQLite table schema based on JSON keys
  3. Ingest rows (insert/update)
  4. Load to BigQuery table `<BIGQUERY_DATASET_ID>.<table>` (WRITE_TRUNCATE)

Run load only:
```bash
python main.py --db-sync-only
```

Run full pipeline:
```bash
python main.py
```

---

## Local Setup

### Requirements
- Python 3.9+
- Google Cloud project with BigQuery and GCS enabled
- Service account with Storage (read/write) and BigQuery (dataset read/write) permissions

### Environment
Create a `.env` for extraction:
```env
TWITTER_USERNAME=...
TWITTER_EMAIL=...
TWITTER_PASSWORD=...
TWITTER_TOTP_SECRET=...
```
Set GCP envs (shell or CI/CD secrets) for load:
```bash
export GCP_PROJECT_ID=your-project-id
export GCS_BUCKET_NAME=your-bucket
export BIGQUERY_DATASET_ID=your_dataset
export GOOGLE_APPLICATION_CREDENTIALS=/abs/path/to/service-account.json
```

### Install and run
```bash
pip install -r requirements.txt
python main.py  # or --ingest-only / --db-sync-only
```

---

## Docker

### Image contents (`Dockerfile`)
- Base: `python:3.9-slim`
- Installs Google Cloud SDK
- Installs Python deps from `requirements.txt`
- Default command: `python main.py`

### Build
```bash
docker build -t tweet-pipeline:latest .
```

### Run: ingestion only (requires Twitter envs)
```bash
docker run --rm \
  -v "$(pwd)/data":/app/data \
  --env-file .env \
  tweet-pipeline:latest \
  python main.py --ingest-only
```

### Run: load only (requires GCP envs and key)
```bash
docker run --rm \
  -v "$(pwd)/data":/app/data \
  -v "/abs/path/key.json":/gcp-key.json \
  -e GOOGLE_APPLICATION_CREDENTIALS=/gcp-key.json \
  -e GCP_PROJECT_ID=your-project-id \
  -e GCS_BUCKET_NAME=your-bucket \
  -e BIGQUERY_DATASET_ID=your_dataset \
  tweet-pipeline:latest \
  python main.py --db-sync-only
```

### docker-compose
A minimal compose exists and mounts the repo into `/app`:
```bash
docker compose up --build
```
Note: Provide required envs via `.env`, `environment:` block, or `--env-file` in your local setup.

---

## Jenkins Pipeline

Pipeline is defined in `Jenkinsfile` with these stages:

1) Build
- Builds the image `tweet-pipeline` from the repo root.

2) run application
- Runs the default container command (`python main.py`) which executes ingest + load.
  - Ensure Twitter and GCP envs are available at runtime if you expect the full pipeline to succeed here.

3) sync to GCS
- Uses `google/cloud-sdk:latest` to authenticate with a file credential and `gsutil rsync -r ./data gs://${GCS_BUCKET}/data`.
- Jenkins requirements:
  - File credential with ID `gcp_service_acct_proj_belayground` (referenced as `GCP_CREDENTIALS_ID`).
  - Pipeline env `GCS_BUCKET` (default `belayground_db`).

4) process and load to BQ
- Runs the app image and executes `python src/data_jobs/db_manager.py` with `GOOGLE_APPLICATION_CREDENTIALS` mounted.
- Ensure the following are set in the container environment via Jenkins Global Env, Folder/Job Env, or by extending the pipeline:
  - `GCP_PROJECT_ID`
  - `GCS_BUCKET_NAME`
  - `BIGQUERY_DATASET_ID`

### Typical Jenkins configuration
- Add a File credential for the service account key with ID `gcp_service_acct_proj_belayground`.
- Option 1 (recommended): Configure global envs in Jenkins (Manage Jenkins → System → Global properties):
  - `GCP_PROJECT_ID`, `GCS_BUCKET_NAME`, `BIGQUERY_DATASET_ID`
  - (Optional) Twitter envs for ingestion if running in CI: `TWITTER_USERNAME`, `TWITTER_EMAIL`, `TWITTER_PASSWORD`, `TWITTER_TOTP_SECRET`
- Option 2: Modify `Jenkinsfile` to pass `withEnv([...])` or `environment { ... }` to the run stages.

---

## Operational Notes
- GCS path convention: `gs://${GCS_BUCKET_NAME}/data/raw/*.json`
- BigQuery write mode: `WRITE_TRUNCATE` per table load
- SQLite is ephemeral (temp directory) and used for schema evolution
- Logs: `data/processed/logging.json`

## Troubleshooting
- Twitter login failures: validate `.env` and 2FA secret; watch for account lock/suspension
- Rate limits: the extractor waits and resumes; repeated 429s will slow runs
- GCP auth: ensure `GOOGLE_APPLICATION_CREDENTIALS` is set and the key has Storage+BigQuery permissions
- Missing envs in CI: set `GCP_PROJECT_ID`, `GCS_BUCKET_NAME`, `BIGQUERY_DATASET_ID` for load; Twitter envs for extract if needed