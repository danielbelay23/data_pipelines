import os
import sqlite3
import json
import tempfile
from google.cloud import storage, bigquery
from google.api_core.exceptions import NotFound
from data_jobs import GCP_PROJECT_ID, GCS_BUCKET_NAME, BIGQUERY_DATASET_ID

storage_client = storage.Client(project=GCP_PROJECT_ID)
bq_client = bigquery.Client(project=GCP_PROJECT_ID)

def download_from_gcs(source_blob_name, destination_file_name):
    """downloads a file from GCS to a local path."""
    try:
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)
        print(f"Successfully downloaded gs://{GCS_BUCKET_NAME}/{source_blob_name} to {destination_file_name}")
        return True
    except NotFound:
        print(f"Error: File not found in GCS: gs://{GCS_BUCKET_NAME}/{source_blob_name}")
        return False
    except Exception as e:
        print(f"An error occurred during GCS download: {e}")
        return False

def load_db_to_bigquery(db_path, table_name, dataset_id):
    """loads data from a SQLite database table into a BigQuery table."""
    print(f"starting BQ load for '{table_name}' from '{db_path}'")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    if not rows:
        print(f"no data found in SQLite table '{table_name}'. skipping BQ load")
        return

    cursor.execute(f"PRAGMA table_info({table_name})")
    db_schema = cursor.fetchall()
    bq_schema = [bigquery.SchemaField(col[1], 'STRING') for col in db_schema]
    column_names = [col[1] for col in db_schema]
    data_to_load = [dict(zip(column_names, row)) for row in rows]
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_name)
    job_config = bigquery.LoadJobConfig(
        schema=bq_schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    try:
        bq_client.create_dataset(dataset_ref, exists_ok=True)
        load_job = bq_client.load_table_from_json(
            data_to_load,
            table_ref,
            job_config=job_config
        )
        print(f" -> starting BQ load job {load_job.job_id} for table '{table_name}'")

        load_job.result()
        destination_table = bq_client.get_table(table_ref)
        print(f" -> successfully loaded {destination_table.num_rows} rows into '{table_name}'")

    except Exception as e:
        print(f"an error occurred during the BQ load for table '{table_name}': {e}")
    finally:
        conn.close()


def create_connection(db_file):
    """create a database connection to a sqlite database."""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"successfully connected to: {db_file}")
    except sqlite3.Error as e:
        print(e)
    return conn

def create_table(conn, table_name, primary_key):
    """create a table with a primary key if it doesn't exist."""
    try:
        c = conn.cursor()
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                {primary_key} TEXT PRIMARY KEY
            )
        ''')
        print(f"table '{table_name}' loaded successfully")
    except sqlite3.Error as e:
        print(f"error creating table {table_name}: {e}")

def get_all_records_from_json(json_file):
    """
    reads a json file and returns a flat list of records.
    handles a list of objects, a dictionary of lists, or a single object.
    """
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            is_dict_of_lists = bool(data) and all(isinstance(v, list) for v in data.values())

            if is_dict_of_lists:
                all_records = []
                for key, value in data.items():
                    all_records.extend(value)
                return all_records
            else:
                return [data]
        else:
            return []

    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"error reading or parsing json file '{json_file}': {e}")
        return []

def sync_schema(conn, table_name, all_records, primary_key, static_pk_value=None):
    """
    checks for new keys in the records and adds them as new columns
    to the specified table if they don't already exist.
    """
    if not all_records:
        print(f"no records found to sync schema for table '{table_name}'")
        return

    c = conn.cursor()
    json_keys = set()
    for item in all_records:
        json_keys.update(item.keys())

    if static_pk_value:
        json_keys.add(primary_key)

    c.execute(f"PRAGMA table_info({table_name})")
    db_columns = {row[1] for row in c.fetchall()}
    new_columns = json_keys - db_columns

    if new_columns:
        print(f"new fields found for '{table_name}': {', '.join(new_columns)}. syncing schema...")
        for column in new_columns:
            try:
                c.execute(f'ALTER TABLE {table_name} ADD COLUMN {column} TEXT')
                print(f" -> added column '{column}' to the '{table_name}' table.")
            except sqlite3.Error as e:
                print(f"error adding column {column} to {table_name}: {e}")
        conn.commit()
    else:
        print(f"schema for '{table_name}' is already up-to-date.")

def ingest_data(conn, table_name, all_records, primary_key, static_pk_value=None):
    """read data from a list of records and insert or update them in the specified table."""
    if not all_records:
        print(f"no records to ingest for table '{table_name}'")
        return

    c = conn.cursor()
    new_count = 0
    updated_count = 0

    for record in all_records:
        if static_pk_value:
            record[primary_key] = static_pk_value

        sanitized_record = {}
        for key, value in record.items():
            if isinstance(value, (dict, list)):
                sanitized_record[key] = json.dumps(value)
            else:
                sanitized_record[key] = value

        pk_value = sanitized_record.get(primary_key)
        if pk_value is None:
            print(f"record was skipped bc pk is missing. pk:'{primary_key}', record: {sanitized_record}")
            continue

        c.execute(f"SELECT {primary_key} FROM {table_name} WHERE {primary_key} = ?", (pk_value,))
        result = c.fetchone()

        columns = ', '.join(sanitized_record.keys())
        placeholders = ', '.join('?' * len(sanitized_record))
        values = list(sanitized_record.values())

        if result is None:
            try:
                c.execute(f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})", values)
                new_count += 1
            except sqlite3.Error as e:
                print(f"error inserting record {pk_value} into {table_name}: {e}")
        else:
            update_keys = [k for k in sanitized_record.keys() if k != primary_key]
            if not update_keys:
                continue
            update_clause = ', '.join([f"{key} = ?" for key in update_keys])
            update_values = [sanitized_record[k] for k in update_keys] + [pk_value]

            try:
                c.execute(f"UPDATE {table_name} SET {update_clause} WHERE {primary_key} = ?", update_values)
                if c.rowcount > 0:
                    updated_count += 1
            except sqlite3.Error as e:
                print(f"error updating record {pk_value} in {table_name}: {e}")

    conn.commit()
    print(f"ingestion summary for '{table_name}'")
    if new_count > 0:
        print(f"successfully added {new_count} new records")
    if updated_count > 0:
        print(f"successfully updated {updated_count} existing records")
    if new_count == 0 and updated_count == 0:
        print("no new or updated records to process")
    print("-" * (len(table_name) + 28))


def main():
    """main function to download from GCS, process into SQLite, and load to BigQuery."""
    gcs_raw_data_path = 'data/raw'
    tables_to_process = [
        {
            "table_name": "following",
            "gcs_source_file": f"{gcs_raw_data_path}/following.json",
            "primary_key": "id"
        },
        {
            "table_name": "tweets",
            "gcs_source_file": f"{gcs_raw_data_path}/tweets.json",
            "primary_key": "id"
        },
        {
            "table_name": "cookies",
            "gcs_source_file": f"{gcs_raw_data_path}/cookies.json",
            "primary_key": "session_id",
            "static_pk_value": "active_session"
        }
    ]

    with tempfile.TemporaryDirectory() as temp_dir:
        for config in tables_to_process:
            table_name = config["table_name"]
            gcs_source_file = config["gcs_source_file"]
            primary_key = config["primary_key"]
            static_pk_value = config.get("static_pk_value")
            local_json_path = os.path.join(temp_dir, os.path.basename(gcs_source_file))
            local_db_path = os.path.join(temp_dir, f"{table_name}.db")

            print(f"\n processing table '{table_name}'")
            if not download_from_gcs(gcs_source_file, local_json_path):
                continue

            conn = create_connection(local_db_path)
            if conn is not None:
                create_table(conn, table_name, primary_key)
                all_records = get_all_records_from_json(local_json_path)
                if all_records:
                    sync_schema(conn, table_name, all_records, primary_key, static_pk_value)
                    ingest_data(conn, table_name, all_records, primary_key, static_pk_value)
                conn.close()
                print(f"local table '{local_db_path}' created successfully")
                load_db_to_bigquery(local_db_path, table_name, BIGQUERY_DATASET_ID)
            else:
                print(f"DB connection failed for {table_name}")

    print("\n DB sync complete")

if __name__ == '__main__':
    main()
