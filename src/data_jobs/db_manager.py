import os
import sqlite3
import json
from src.data_jobs import PROCESSED_DIR, FOLLOWING_FILE, TWEETS_FILE, COOKIES_FILE

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
        print(f"no records found to sync schema for table '{table_name}'.")
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
        print(f"No records to ingest for table '{table_name}'.")
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

    print(f"--- ingestion summary for '{table_name}' ---")
    if new_count > 0:
        print(f"successfully added {new_count} new records.")
    if updated_count > 0:
        print(f"successfully updated {updated_count} existing records.")
    if new_count == 0 and updated_count == 0:
        print("no new or updated records to process.")
    print("-" * (len(table_name) + 28))

def main():
    """main function to run the database operations for all configured tables."""
    tables_to_process = [
        {
            "table_name": "following",
            "json_file": FOLLOWING_FILE,
            "primary_key": "id"
        },
        {
            "table_name": "tweets",
            "json_file": TWEETS_FILE,
            "primary_key": "id"
        },
        {
            "table_name": "cookies",
            "json_file": COOKIES_FILE,
            "primary_key": "session_id",
            "static_pk_value": "active_session"
        }
    ]

    for config in tables_to_process:
        table_name = config["table_name"]
        json_file = config["json_file"]
        primary_key = config["primary_key"]
        static_pk_value = config.get("static_pk_value")
        database_file = os.path.join(PROCESSED_DIR, f"{table_name}.db")
        print(f"\nprocessing '{json_file}' into database '{database_file}'...")

        conn = create_connection(database_file)
        if conn is not None:
            create_table(conn, table_name, primary_key)
            all_records = get_all_records_from_json(json_file)
            if all_records:
                sync_schema(conn, table_name, all_records, primary_key, static_pk_value)
                ingest_data(conn, table_name, all_records, primary_key, static_pk_value)
            conn.close()
            print(f"database '{database_file}' complete. connection closed.")
        else:
            print(f"db connection failed for {table_name}")
    print("\nall db sync'd successfully.")

if __name__ == '__main__':
    main()