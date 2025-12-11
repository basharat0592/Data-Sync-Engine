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
# Caches
# -------------------------
agency_id_cache: Dict[str, int] = {}
insured_id_cache: Dict[str, int] = {}

def preload_agency_mappings(db_name: str):
    global agency_id_cache
    agency_id_cache.clear()
    print(f"  Preloading agency mappings for {db_name}...", end=" ")
    pg_cur.execute("""
        SELECT momentum_agency_id, agency_id 
        FROM domain.agencies 
        WHERE db_name = %s AND momentum_agency_id IS NOT NULL
    """, (db_name,))
    for guid, pg_id in pg_cur.fetchall():
        guid_str = str(guid).strip().upper().replace("{", "").replace("}", "")
        agency_id_cache[guid_str] = int(pg_id)
    print(f"loaded {len(agency_id_cache)}")

def preload_insured_mappings(db_name: str):
    global insured_id_cache
    insured_id_cache.clear()
    print(f"  Preloading insured mappings for {db_name}...", end=" ")
    # ← THIS WAS THE BUG — MISSING "SELECT"!
    pg_cur.execute("""
        SELECT momentum_insured_id, insured_id 
        FROM domain.insureds
        WHERE db_name = %s AND momentum_insured_id IS NOT NULL
    """, (db_name,))
    for guid, pg_id in pg_cur.fetchall():
        guid_str = str(guid).strip().upper().replace("{", "").replace("}", "")
        insured_id_cache[guid_str] = int(pg_id)
    print(f"loaded {len(insured_id_cache)}")

# -------------------------
# Transforms (only what you use)
# -------------------------
def clean_phone(row_dict: dict, db_name: str) -> str:
    val = row_dict.get("Phone") or row_dict.get("CellPhone") or ""
    s = str(val).strip()
    for p in [" Ext.", " ext.", " x", " X", " - "]:
        if p in s:
            s = s.split(p, 1)[0]
    return s.strip()

def clean_zipcode(row_dict: dict, db_name: str) -> str:
    return str(row_dict.get("ZipCode") or "")[:20]

def lookup_agency_id(row_dict: dict, db_name: str) -> int:
    raw = row_dict.get("InsuranceAgencyId")
    if not raw: return 0
    guid_str = str(raw).strip().upper().replace("{", "").replace("}", "")
    return agency_id_cache.get(guid_str)

def lookup_insured_id(row_dict: dict, db_name: str) -> int:
    raw = row_dict.get("TruckingCompanyId")
    if not raw: return 0
    guid_str = str(raw).strip().upper().replace("{", "").replace("}", "")
    return insured_id_cache.get(guid_str)

# cache for SQL Server states keyed by normalized GUID (32-char hex)
state_cache = {}  # { GUID32: abbreviation }

def load_state_cache(sql_cursor):
    """
    Load SQL Server state IDs and abbreviations into memory.
    Includes debug prints to confirm loaded key formats.
    """
    print(f"  Preloading states from database...", end=" ")
    sql_cursor.execute("SELECT Id, AbbreviationCode FROM states")
    rows = sql_cursor.fetchall()

    for row in rows:
        raw_id = row[0]
        abbr = row[1] if row[1] else "NA"
        if raw_id:
            state_cache[raw_id] = abbr
        else:
            print("[WARN] Value NULL:", raw_id)


def state_abbreviation_lookup(row_dict: dict, db_name: str) -> str:
    """
    Lookup state abbreviation
    """
    raw = row_dict.get("StateId")
    abbr = state_cache.get(raw)
    return abbr if abbr else "NA"

sql_state_conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={sql_conf['server']};"
    "DATABASE=NewNowCertsLocal1;"
    f"UID={sql_conf['user']};"
    f"PWD={sql_conf['password']};"
    f"TrustServerCertificate=yes;"
)
sql_state_conn = pyodbc.connect(sql_state_conn_str, autocommit=False)
cursor = sql_state_conn.cursor()
load_state_cache(cursor)

TRANSFORMS = {
    "lookup_agency_id": lookup_agency_id,
    "lookup_insured_id": lookup_insured_id,
    "clean_phone": clean_phone,
    "zipcode": clean_zipcode,
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
# Safe COPY
# -------------------------
def copy_chunk_to_target(target_table: str, columns: List[str], rows: List[Dict[str, Any]], include_db_name: bool, db_name: str):
    if not rows:
        return
    print(f"Preparing to copy {len(rows)} rows into {target_table}...")
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
    print(f"Copying {len(rows)} rows to PostgreSQL table {target_table} (this may take a while for large chunks)...")
    pg_cur.copy_expert(f"COPY {target_table} ({', '.join(final_cols)}) FROM STDIN WITH (FORMAT csv, NULL '')", buf)
    pg_conn.commit()
    buf.close()
    print(f"Finished copying {len(rows)} rows into {target_table}")

# -------------------------
# MAIN: Only change = computed columns for Policies
# -------------------------
def stream_source_to_target(
    db_name: str,
    source_table: str,
    source_cols: List[str],
    col_map: Dict[str, str],
    target_table: str,
    chunk_size: int = 100000,
    where_clause: Optional[str] = None,
    where_params: Optional[list] = None,
    include_db_name: bool = True,
    transforms: Optional[Dict[str, str]] = None,
    table_config: dict = None
):
    print(f"\nStreaming from {source_table} → {target_table} (chunk size: {chunk_size})")
    sqlserver_use_db(db_name)
    cur = sql_conn.cursor()
    cur.arraysize = chunk_size

    if source_table == "dbo.Policies":
        computed_sql = """,
            (SELECT STRING_AGG(lob.[Name], ', ') 
             FROM XRefPolicyLineOfBusinesses x
             INNER JOIN LineOfBusinesses lob ON x.LineOfBusinessId = lob.Id
             WHERE x.PolicyId = p.Id
            ) AS line_of_business,

            (SELECT COALESCE(SUM(e.Amount), 0.0)
             FROM Endorsements e
             WHERE e.PolicyId = p.Id AND e.PremiumType = 0
            ) AS total_premium,

            (SELECT COALESCE(SUM(
                CASE WHEN eac.CommissionValue IS NOT NULL THEN
                    CASE WHEN eac.CommissionType = 1 THEN eac.CommissionValue
                         ELSE (eac.CommissionValue / 100.0) * e.Amount END
                ELSE 0.0 END), 0.0)
             FROM Endorsements e
             INNER JOIN EndorsementAgencyCommissions eac ON eac.EndorsementId = e.Id
             WHERE e.PolicyId = p.Id
            ) AS agency_commission,

            COALESCE(parent.Name, carrier.Name, '') AS carrier_name
        """
        joins = """
            LEFT JOIN TruckingCompanies carrier ON p.NAICid = carrier.Id
            LEFT JOIN TruckingCompanies parent ON carrier.ParentId = parent.Id
        """
        select_cols = ", ".join(f"p.[{c}]" for c in source_cols)
        q = f"SELECT {select_cols}{computed_sql} FROM dbo.Policies p {joins}"
        final_source_cols = source_cols + ['line_of_business', 'total_premium', 'agency_commission', 'carrier_name']
    else:
        select_cols = ", ".join(f"[{c}]" for c in source_cols)
        q = f"SELECT {select_cols} FROM {source_table}"
        final_source_cols = source_cols

    if where_clause:
        # Determine alias
        qualifier = "p" if source_table == "dbo.Policies" else source_table

        # Split by whitespace and replace unqualified ChangeDate
        tokens = where_clause.split()
        wc_tokens = []
        for t in tokens:
            if "ChangeDate" in t and "." not in t:
                # Handle brackets or bare
                t = t.replace("ChangeDate", f"[{qualifier}].[ChangeDate]") if not t.startswith("[") else f"{qualifier}.[ChangeDate]"
            wc_tokens.append(t)

        wc = " ".join(wc_tokens)
        q += f" WHERE {wc}"

    cur.execute(q, where_params or [])

    target_columns = list(col_map.values())
    defaults = (table_config or {}).get("default_values", {})
    target_columns += [c for c in defaults if c not in target_columns]

    if source_table == "dbo.Policies":
        for col in ['line_of_business', 'total_premium', 'agency_commission', 'carrier_name']:
            if col not in target_columns:
                target_columns.append(col)

    total = 0
    chunk = []

    while True:
        rows = cur.fetchmany(chunk_size)
        if not rows:
            if chunk:
                copy_chunk_to_target(target_table, target_columns, chunk, include_db_name, db_name)
                total += len(chunk)
                print(f"   Copied {total:,} rows (computed fields filled)")
            break
        print(f"Fetched {len(rows)} rows from source.")
        for row in rows:
            row_dict = dict(zip(final_source_cols, row))

            if "TruckingCompanyContacts" in source_table and not row_dict.get("TruckingCompanyId"):
                continue

            transformed = {}
            if transforms:
                for tgt_col, func_name in transforms.items():
                    if func_name in TRANSFORMS:
                        try:
                            transformed[tgt_col] = TRANSFORMS[func_name](row_dict, db_name)
                        except:
                            transformed[tgt_col] = None

            mapped_row = {}
            for src_col, tgt_col in col_map.items():
                mapped_row[tgt_col] = transformed.get(tgt_col, row_dict.get(src_col))

            for col, val in defaults.items():
                mapped_row[col] = val

            if source_table == "dbo.Policies":
                mapped_row['line_of_business'] = row_dict.get('line_of_business', '')
                mapped_row['total_premium'] = row_dict.get('total_premium', 0.0)
                mapped_row['agency_commission'] = row_dict.get('agency_commission', 0.0)
                mapped_row['carrier_name'] = row_dict.get('carrier_name', '')

            chunk.append(mapped_row)

        if len(chunk) >= chunk_size:
            copy_chunk_to_target(target_table, target_columns, chunk, include_db_name, db_name)
            total += len(chunk)
            print(f"   Copied {total:,} rows...")
            chunk = []

    cur.close()
    print(f"Finished streaming {total:,} rows from {source_table} → {target_table}")
    return total

# -------------------------
# Sync table
# -------------------------
def sync_table(db_name: str, table_config: dict):
    source = table_config["source"]
    target = table_config["target"]
    col_map = table_config["columns"]
    include_db_name = table_config.get("include_db_name", True)
    transforms = table_config.get("transform", {})
    chunk_size = table_config.get("chunk_size", 100000)
    inc_col = table_config.get("incremental_column", "ChangeDate")

    source_cols = list(col_map.keys())

    print(f"\nSyncing {db_name}.{source} → {target}")

    if source in ["dbo.Agents", "dbo.Policies"]:
        preload_agency_mappings(db_name)
    if source in ["dbo.TruckingCompanyContacts", "dbo.Policies"]:
        preload_insured_mappings(db_name)

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

# -------------------------
# Main
# -------------------------
def main():
    ensure_sync_state_table()
    print(f"SYNC STARTED at {time.time()}...\n")
    start = time.time()

    for db_name in config["databases"]:
        print(f"\n=== DATABASE: {db_name} ===")
        for table_config in config["tables"]:
            try:
                sync_table(db_name, table_config)
            except Exception as e:
                print(f"ERROR {db_name}.{table_config.get('source')}: {e}")
                import traceback; traceback.print_exc()
                pg_conn.rollback()

    print(f"\nSUCCESS! Finished in {time.time()-start:.1f} seconds")

if __name__ == "__main__":
    main()