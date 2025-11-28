import json
import pyodbc
import psycopg2
import io
import csv
import time
from datetime import datetime

# -------------------------
# Load config
# -------------------------
with open("config_dbs.json", "r") as f:
    config = json.load(f)

sql_conf = config["sqlserver"]
pg_conf = config["postgres"]

# -------------------------
# Connections
# -------------------------
sql_conn_str = (
    f"DRIVER={{{sql_conf['driver']}}};"
    f"SERVER={sql_conf['server']};"
    f"UID={sql_conf['user']};"
    f"PWD={sql_conf['password']};"
)
sql_conn = pyodbc.connect(sql_conn_str, autocommit=False)

pg_conn = psycopg2.connect(
    host=pg_conf["host"],
    port=int(pg_conf.get("port", 5432)),
    database=pg_conf["database"],
    user=pg_conf["user"],
    password=pg_conf["password"]
)
pg_conn.autocommit = False
pg_cur = pg_conn.cursor()

# -------------------------
# Sync state helpers
# -------------------------
def ensure_sync_state_table():
    pg_cur.execute("""
    CREATE TABLE IF NOT EXISTS sync_state (
        db_name TEXT NOT NULL,
        table_name TEXT NOT NULL,
        last_sync TIMESTAMP WITH TIME ZONE,
        PRIMARY KEY (db_name, table_name)
    );
    """)
    pg_conn.commit()

def get_last_sync(db, table):
    pg_cur.execute("""
        SELECT last_sync FROM sync_state
        WHERE db_name=%s AND table_name=%s
    """, (db, table))
    row = pg_cur.fetchone()
    return row[0] if row else None

def save_last_sync(db, table, timestamp):
    pg_cur.execute("""
        INSERT INTO sync_state(db_name, table_name, last_sync)
        VALUES (%s, %s, %s)
        ON CONFLICT (db_name, table_name)
        DO UPDATE SET last_sync = EXCLUDED.last_sync
    """, (db, table, timestamp))
    pg_conn.commit()

# -------------------------
# Helpers
# -------------------------
def sqlserver_use_db(db_name):
    cur = sql_conn.cursor()
    cur.execute(f"USE [{db_name}]")
    cur.close()

def normalize_pk(pk):
    if isinstance(pk, list):
        return [p.lower() for p in pk]
    return [pk.lower()]

def query_source_max(source_table, inc_col):
    cur = sql_conn.cursor()
    cur.execute(f"SELECT MAX([{inc_col}]) FROM {source_table}")
    val = cur.fetchone()[0]
    cur.close()
    return val

# -------------------------
# Copy chunk to target + upsert
# -------------------------
def copy_chunk_to_target(target_table, columns, rows, pk_cols, cast_uuid=False):
    if not rows:
        return
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)

    for r in rows:
        processed = []
        for v in r:
            if v is None:
                processed.append('')
            elif isinstance(v, (bytes, bytearray)):
                processed.append(v.decode('utf-8', errors='ignore').replace('\x00',''))
            elif isinstance(v, datetime):
                processed.append(v.isoformat(sep=' '))
            else:
                processed.append(str(v).replace('\x00',''))
        writer.writerow(processed)

    buf.seek(0)
    col_list = ", ".join([c.lower() for c in columns])
    pg_cur.copy_expert(f"COPY {target_table} ({col_list}) FROM STDIN WITH (FORMAT csv)", buf)
    pg_conn.commit()
    buf.close()

# -------------------------
# Streaming from SQL Server
# -------------------------
def stream_source_to_target(db_name, source_table, source_cols, target_table, target_columns, pk_cols, chunk_size=50000, where_clause=None, where_params=None, cast_uuid=False):
    sqlserver_use_db(db_name)
    cur = sql_conn.cursor()
    cur.arraysize = chunk_size

    # Select columns from source
    select_cols = ", ".join([f"[{c}]" for c in source_cols])
    q = f"SELECT {select_cols} FROM {source_table}"
    if where_clause:
        q += " WHERE " + where_clause

    cur.execute(q, *(where_params or []))
    total = 0

    # Ensure db_name is included in target columns
    if "db_name" not in target_columns:
        target_columns.append("db_name")

    while True:
        rows = cur.fetchmany(chunk_size)
        if not rows:
            break

        rows_tuples = []
        for r in rows:
            row_list = list(r)
            # Add db_name as last column
            row_list.append(db_name)
            rows_tuples.append(tuple(row_list))

        copy_chunk_to_target(target_table, target_columns, rows_tuples, pk_cols, cast_uuid)
        total += len(rows_tuples)
        print(f"  Copied {total} rows to {target_table} (chunk size {len(rows_tuples)})")

    cur.close()
    return total

# -------------------------
# Run job
# -------------------------
def run_job(job):
    db_name = job["database"]
    print(f"\n=== Processing DB: {db_name} ===")
    sqlserver_use_db(db_name)

    for table in job["tables"]:
        source = table["source"]
        target = table["target"]
        pk = normalize_pk(table["pk"])
        inc_col = table["incremental_column"]
        col_map = table["columns"]
        chunk_size = table.get("chunk_size", 50000)

        source_cols = list(col_map.keys())
        target_columns = [col_map[k] for k in source_cols]

        print(f"\n-- Syncing {source} -> {target}")
        last_sync = get_last_sync(db_name, source)
        full_load = last_sync is None

        cast_uuid = any(c.lower() == 'id' for c in pk)

        if full_load:
            print("  Last sync: None -> performing FULL load (direct COPY).")
            rows_copied = stream_source_to_target(db_name, source, source_cols, target, target_columns, pk, chunk_size=chunk_size, cast_uuid=cast_uuid)
            print(f"  Finished copying {rows_copied} rows into {target}")
            new_last_sync = query_source_max(source, inc_col)
            if new_last_sync:
                save_last_sync(db_name, source, new_last_sync)
                print(f"  Updated last_sync to {new_last_sync}")
        else:
            print(f"  Last sync: {last_sync} -> performing INCREMENTAL load.")
            where_clause = f"[{inc_col}] > ?"
            where_params = [last_sync]
            rows_copied = stream_source_to_target(db_name, source, source_cols, target, target_columns, pk, chunk_size=chunk_size, where_clause=where_clause, where_params=where_params, cast_uuid=cast_uuid)
            print(f"  Finished copying {rows_copied} rows into {target}")
            if rows_copied > 0:
                new_last_sync = query_source_max(source, inc_col)
                if new_last_sync:
                    save_last_sync(db_name, source, new_last_sync)
                    print(f"  Updated last_sync to {new_last_sync}")
            else:
                print("  No new rows to merge.")

# -------------------------
# Main
# -------------------------
def main():
    ensure_sync_state_table()
    start = time.time()
    # for job in config["jobs"]:
    #     run_job(job)
    for db_name in config["databases"]:
        job = {
            "database": db_name,
            "tables": config["tables"]
        }
        run_job(job)
    elapsed = time.time() - start
    print(f"\nAll done. Elapsed: {elapsed:.1f}s")

if __name__ == "__main__":
    main()
