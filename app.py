import pyodbc
import psycopg2
import pandas as pd
import json
from datetime import datetime

# -----------------------------------------------
# Load config
# -----------------------------------------------
with open("config.json", "r") as f:
    config = json.load(f)

sql_conf = config["sqlserver"]
pg_conf = config["postgres"]

# -----------------------------------------------
# SQL Server connection
# -----------------------------------------------
sql_conn_str = (
    f"DRIVER={{{sql_conf['driver']}}};"
    f"SERVER={sql_conf['server']};"
    f"UID={sql_conf['user']};"
    f"PWD={sql_conf['password']};"
)
sql_conn = pyodbc.connect(sql_conn_str)

# -----------------------------------------------
# PostgreSQL connection
# -----------------------------------------------
pg_conn = psycopg2.connect(
    host=pg_conf["host"],
    port=pg_conf["port"],
    database=pg_conf["database"],
    user=pg_conf["user"],
    password=pg_conf["password"]
)
pg_cur = pg_conn.cursor()

# -----------------------------------------------
# Sync metadata table
# -----------------------------------------------
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

# -----------------------------------------------
# Upsert rows into Postgres
# -----------------------------------------------
def upsert_dataframe(df, target_table, pk):
    if df.empty:
        return

    columns = list(df.columns)
    col_list = ",".join(columns)
    placeholders = ",".join(["%s"] * len(columns))

    update_clause = ",".join([
        f"{col}=EXCLUDED.{col}" for col in columns if col != pk
    ])

    query = f"""
        INSERT INTO {target_table} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT ({pk})
        DO UPDATE SET {update_clause};
    """

    data = [tuple(row) for row in df.to_numpy()]
    pg_cur.executemany(query, data)
    pg_conn.commit()

# -----------------------------------------------
# MAIN SYNC LOOP
# -----------------------------------------------
for job in config["jobs"]:
    db_name = job["database"]
    print(f"--- Processing DB: {db_name} ---")

    sql_conn.execute(f"USE {db_name}")

    for table in job["tables"]:
        source = table["source"]
        target = table["target"]
        pk = table["pk"]
        inc = table["incremental_column"]
        col_map = table["columns"]

        print(f"Syncing table: {source}")

        last_sync = get_last_sync(db_name, source)

        # Build SELECT with mapped column names
        sql_cols = ", ".join(col_map.keys())
        query = f"SELECT {sql_cols} FROM {source}"

        if last_sync:
            query += f" WHERE {inc} > ?"
            df = pd.read_sql(query, sql_conn, params=[last_sync])
        else:
            df = pd.read_sql(query, sql_conn)

        print(f"Rows fetched: {len(df)}")

        if df.empty:
            continue

        # Rename columns to PostgreSQL names
        df.rename(columns=col_map, inplace=True)

        # Perform UPSERT
        upsert_dataframe(df, target, pk)

        # Update sync time
        max_value = df[col_map[inc]].max()
        save_last_sync(db_name, source, max_value)

print("All Done!")
