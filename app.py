import json
import pyodbc
import psycopg2
import io
import csv
import time
from datetime import datetime
from typing import List, Dict, Any, Optional

# -------------------------
# Load config
# -------------------------
with open("config.json", "r") as f:
    config = json.load(f)

sql_conf = config["sqlserver"]
pg_conf = config["postgres"]

# -------------------------
# Connections
# -------------------------
sql_conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={sql_conf['server']};"
    f"UID={sql_conf['user']};"
    f"PWD={sql_conf['password']};"
    f"TrustServerCertificate=yes;"
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
# Global Agency Cache
# -------------------------
agency_id_cache: Dict[str, int] = {}

def preload_agency_mappings(db_name: str):
    global agency_id_cache
    agency_id_cache.clear()
    print(f"  Preloading agency mappings for {db_name}...", end=" ")
    pg_cur.execute("""
        SELECT momentum_agency_id, agency_id 
        FROM domain.agencies 
        WHERE db_name = %s AND momentum_agency_id IS NOT NULL
    """, (db_name,))
    count = 0
    for guid, pg_id in pg_cur.fetchall():
        guid_str = str(guid).strip().upper().replace("{", "").replace("}", "")
        agency_id_cache[guid_str] = int(pg_id)
        count += 1
    print(f"Loaded {count} agencies")

def clean_phone(row_dict: dict, db_name: str) -> str:
    """Remove phone extensions like 'Ext. 104' so it fits in varchar(20)"""
    val = row_dict.get("Phone") or row_dict.get("CellPhone") or ""
    if not val:
        return ""
    s = str(val).strip()
    for pattern in [" Ext.", " ext.", " x", " X", " - ", " Ext ", " ext "]:
        if pattern in s:
            s = s.split(pattern, 1)[0]  # split only once
    return s.strip()

def lookup_agency_id(row_dict: dict, db_name: str) -> int:
    raw = row_dict.get("InsuranceAgencyId")
    if not raw:
        raise ValueError(f"Agent {row_dict.get('Id')} has NULL InsuranceAgencyId in {db_name}")
    if isinstance(raw, (bytes, bytearray)):
        guid_str = ''.join(f'{b:02x}' for b in raw).upper()
    else:
        guid_str = str(raw).strip().upper().replace("{", "").replace("}", "")
    pg_id = agency_id_cache.get(guid_str)
    if pg_id is None:
        raise ValueError(f"Agency not found: InsuranceAgencyId={guid_str} (db: {db_name})")
    return pg_id

# cache for SQL Server states keyed by GUID string (uppercase, no braces)
state_cache = {}   # { GUID: abbreviation_or_NA }

def load_state_cache(sql_cursor):
    """
    Load SQL Server states table into cache once.
    Expected columns: Id (uniqueidentifier), Name, AbbreviationCode.
    """
    global state_cache

    sql_cursor.execute("SELECT Id, AbbreviationCode FROM states")
    rows = sql_cursor.fetchall()

    for row in rows:
        raw_id = row[0]
        abbr = row[1]

        # Convert SQL Server uniqueidentifier to uppercase string key
        if isinstance(raw_id, (bytes, bytearray)):
            guid_str = ''.join(f'{b:02x}' for b in raw_id).upper()
        else:
            guid_str = str(raw_id).strip().upper().replace("{", "").replace("}", "")

        # If AbbreviationCode is NULL → use "NA"
        state_cache[guid_str] = abbr if abbr else "NA"


def state_abbreviation_lookup(row_dict: dict, db_name: str) -> str:
    """
    Look up the state abbreviation from cached states.
    Input row_dict must contain 'StateId'.
    Returns abbreviation, or 'NA' if NULL or not found.
    """
    raw = row_dict.get("StateId")

    # If NULL in source return NA
    if not raw:
        return "NA"

    # Normalize GUID format
    if isinstance(raw, (bytes, bytearray)):
        guid_str = ''.join(f'{b:02x}' for b in raw).upper()
    else:
        guid_str = str(raw).strip().upper().replace("{", "").replace("}", "")

    # Lookup
    abbr = state_cache.get(guid_str)
    if abbr is None:
        # not found in states table → treat as NA
        return "NA"

    return abbr

TRANSFORMS = {
    "lookup_agency_id": lookup_agency_id,
    "clean_phone": clean_phone,
    "state_abbreviation_lookup": state_abbreviation_lookup
}

# -------------------------
# Sync state
# -------------------------
def ensure_sync_state_table():
    pg_cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_state (
            db_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
            last_sync TIMESTAMPTZ,
            PRIMARY KEY (db_name, table_name)
        );
    """)
    pg_conn.commit()

def get_last_sync(db: str, table: str) -> Optional[datetime]:
    pg_cur.execute("SELECT last_sync FROM sync_state WHERE db_name=%s AND table_name=%s", (db, table))
    row = pg_cur.fetchone()
    return row[0] if row else None

def save_last_sync(db: str, table: str, ts: datetime):
    pg_cur.execute("""
        INSERT INTO sync_state (db_name, table_name, last_sync)
        VALUES (%s, %s, %s)
        ON CONFLICT (db_name, table_name) DO UPDATE SET last_sync = EXCLUDED.last_sync
    """, (db, table, ts))
    pg_conn.commit()

def sqlserver_use_db(db_name: str):
    cur = sql_conn.cursor()
    cur.execute(f"USE [{db_name}]")
    cur.close()

def query_source_max(source_table: str, inc_col: str) -> Optional[datetime]:
    cur = sql_conn.cursor()
    cur.execute(f"SELECT MAX([{inc_col}]) FROM {source_table}")
    val = cur.fetchone()[0]
    cur.close()
    return val

# -------------------------
# COPY chunk
# -------------------------
def copy_chunk_to_target(target_table: str, columns: List[str], rows: List[Dict[str, Any]], include_db_name: bool, db_name: str):
    if not rows:
        return

    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    final_cols = columns + (["db_name"] if include_db_name else [])

    for row_dict in rows:
        values = []
        for col in columns:
            val = row_dict.get(col)
            if val is None:
                values.append("")
            elif isinstance(val, (bytes, bytearray)):
                values.append(val.decode("utf-8", errors="ignore").replace("\x00", ""))
            elif isinstance(val, datetime):
                values.append(val.isoformat(sep=" "))
            elif isinstance(val, bool):
                values.append("t" if val else "f")
            else:
                values.append(str(val).replace("\x00", ""))
        if include_db_name:
            values.append(db_name)
        writer.writerow(values)

    buf.seek(0)
    col_list = ", ".join(final_cols)
    pg_cur.copy_expert(f"COPY {target_table} ({col_list}) FROM STDIN WITH (FORMAT csv, NULL '')", buf)
    pg_conn.commit()
    buf.close()

# -------------------------
# Streaming with transforms + defaults
# -------------------------
def stream_source_to_target(
    db_name: str,
    source_table: str,
    source_cols: List[str],
    col_map: Dict[str, str],
    target_table: str,
    chunk_size: int = 50000,
    where_clause: Optional[str] = None,
    where_params: Optional[list] = None,
    include_db_name: bool = True,
    transforms: Optional[Dict[str, str]] = None,
    table_config: dict = None
):
    sqlserver_use_db(db_name)
    cur = sql_conn.cursor()
    cur.arraysize = chunk_size

    select_cols = ", ".join(f"[{c}]" for c in source_cols)
    q = f"SELECT {select_cols} FROM {source_table}"
    if where_clause:
        q += f" WHERE {where_clause}"

    cur.execute(q, where_params or [])

    # CRITICAL: target_columns must include default_values keys (e.g. role_name)
    base_columns = list(col_map.values())
    default_columns = list((table_config or {}).get("default_values", {}).keys())
    target_columns = base_columns + [c for c in default_columns if c not in base_columns]

    total = 0
    chunk = []

    while True:
        rows = cur.fetchmany(chunk_size)
        if not rows:
            if chunk:
                copy_chunk_to_target(target_table, target_columns, chunk, include_db_name, db_name)
                total += len(chunk)
                print(f"  Copied {total} rows to {target_table}")
            break

        for row in rows:
            row_dict = dict(zip(source_cols, row))

            # Apply transforms
            transformed = {}
            if transforms:
                for tgt_col, func_name in transforms.items():
                    if func_name in TRANSFORMS:
                        transformed[tgt_col] = TRANSFORMS[func_name](row_dict, db_name)

            # Build mapped row
            mapped_row = {}
            for src_col, tgt_col in col_map.items():
                mapped_row[tgt_col] = transformed.get(tgt_col, row_dict.get(src_col))

            # Apply default values
            defaults = (table_config or {}).get("default_values", {})
            for col, val in defaults.items():
                mapped_row[col] = val

            chunk.append(mapped_row)

        if len(chunk) >= chunk_size:
            copy_chunk_to_target(target_table, target_columns, chunk, include_db_name, db_name)
            total += len(chunk)
            print(f"  Copied {total} rows to {target_table}")
            chunk = []

    cur.close()
    return total

# -------------------------
# Sync one table
# -------------------------
def sync_table(db_name: str, table_config: dict):
    source = table_config["source"]
    target = table_config["target"]
    col_map = table_config["columns"]
    include_db_name = table_config.get("include_db_name", True)
    transforms = table_config.get("transform", {})
    chunk_size = table_config.get("chunk_size", 50000)
    inc_col = table_config.get("incremental_column", "ChangeDate")

    source_cols = list(col_map.keys())

    print(f"\n-- Syncing {db_name}.{source} to {target}")

    if source == "dbo.Agents":
        preload_agency_mappings(db_name)

    last_sync = get_last_sync(db_name, source)
    full_load = last_sync is None

    rows = stream_source_to_target(
        db_name=db_name,
        source_table=source,
        source_cols=source_cols,
        col_map=col_map,
        target_table=target,
        chunk_size=chunk_size,
        where_clause=f"[{inc_col}] > ?" if not full_load else None,
        where_params=[last_sync] if not full_load else None,
        include_db_name=include_db_name,
        transforms=transforms,
        table_config=table_config
    )

    if rows > 0 or full_load:
        new_ts = query_source_max(source, inc_col)
        if new_ts:
            save_last_sync(db_name, source, new_ts)
            print(f"  Updated last_sync to {new_ts}")
    else:
        print("  No changes")

# -------------------------
# Main
# -------------------------
def main():
    ensure_sync_state_table()
    print("Starting sync...\n")
    start = time.time()

    for db_name in config["databases"]:
        print(f"\n=== Processing Database: {db_name} ===")
        for table_config in config["tables"]:
            try:
                sync_table(db_name, table_config)
            except Exception as e:
                print(f"ERROR {db_name}.{table_config.get('source', '?')}: {e}")
                pg_conn.rollback()

    elapsed = time.time() - start
    print(f"\nSUCCESS! All done in {elapsed:.1f} seconds")

if __name__ == "__main__":
    main()