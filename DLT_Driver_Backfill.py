# Databricks notebook source
# DBTITLE 1,Imports and Drift ID (from pipeline parameter)
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from pyspark.sql import functions as F

# ── Job Parameters (passed from the triggering Databricks Job) ────────────────────
dbutils.widgets.text("schema_drift_id", "", "Schema Drift ID")
dbutils.widgets.text("adf_job_name", "", "ADF Job Name")


drift_id = dbutils.widgets.get("schema_drift_id").strip()
adf_job_name = dbutils.widgets.get("adf_job_name").strip()

if not drift_id:
    raise ValueError("[Backfill] ERROR: schema_drift_id is required but was not provided.")

print(f"[Backfill] schema_drift_id: {drift_id}")
print(f"[Backfill] adf_job_name:    {adf_job_name}")


# COMMAND ----------

# DBTITLE 1,Environment Config and Utility Functions
# ── Environment Config (dev) ─────────────────────────────────────────────────────────
v_landing_catalog = "dev_landing_dlh_finance"
v_sql_server = "sql-server-dlhfinance"
v_sql_secret = "sql-server-dlhfinance-dev"

print(f"[Backfill] Landing catalog: {v_landing_catalog}")
volume_path = f"/Volumes/{v_landing_catalog}"


audit_catalog = "dev_udl_dlh_finance"

SCHEMA_AUDIT_TABLE = f"{audit_catalog}.finsight.finsight_schema_audit"
print(f"[Backfill] Audit table: {SCHEMA_AUDIT_TABLE}")

PIPELINE_SYSTEM_COLUMNS = {
    "dbx_load_dt", "__start_at", "__end_at",
    "_commit_version", "_commit_timestamp", "__deleted",
    "_rescued_data"
}


def quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def quote_table_name(full_table_name: str) -> str:
    return ".".join(quote_identifier(part) for part in full_table_name.split("."))


def table_exists(full_table_name: str) -> bool:
    try:
        catalog_name, schema_name, table_name = full_table_name.split(".")
        cnt = spark.sql(f"""
            SELECT COUNT(*) AS cnt
            FROM {quote_identifier(catalog_name)}.information_schema.tables
            WHERE table_schema = '{schema_name}'
              AND table_name = '{table_name}'
        """).collect()[0]["cnt"]
        return cnt > 0
    except Exception:
        return False


def get_table_schema_map(full_table_name: str) -> dict:
    if not table_exists(full_table_name):
        return {}

    catalog_name, schema_name, table_name = full_table_name.split(".")
    schema_df = spark.sql(f"""
        SELECT column_name, data_type
        FROM {quote_identifier(catalog_name)}.information_schema.columns
        WHERE table_schema = '{schema_name}'
          AND table_name = '{table_name}'
    """)
    return {row["column_name"]: row["data_type"] for row in schema_df.collect()}


def cast_columns_to_target_schema(df, schema_map: dict):
    if not schema_map:
        return df

    for source_col in df.columns:
        target_type = next((dtype for target_col, dtype in schema_map.items() if target_col.lower() == source_col.lower()), None)
        if target_type:
            df = df.withColumn(source_col, F.col(source_col).cast(target_type))
    return df


def run_backfill_merge(parquet_path: str, full_table_name: str, primary_keys: list, sequence_column: str, audit_table: str, schema_drift_id: str, audit_table_name: str):
    if not table_exists(full_table_name):
        raise ValueError(f"[Backfill] Destination table does not exist: {full_table_name}")

    backfill_df = spark.read.format("parquet").load(parquet_path)
    if not backfill_df.head(1):
        print(f"[Backfill] No parquet rows found at {parquet_path}.")
        return

    target_schema_map = get_table_schema_map(full_table_name)
    target_columns = {col.lower(): col for col in target_schema_map.keys()}
    source_columns = {col.lower(): col for col in backfill_df.columns}

    normalized_primary_keys = [pk.strip() for pk in primary_keys if pk and pk.strip()]
    if not normalized_primary_keys:
        raise ValueError(f"[Backfill] No primary keys supplied for {full_table_name}.")

    missing_keys = [pk for pk in normalized_primary_keys if pk.lower() not in source_columns or pk.lower() not in target_columns]
    if missing_keys:
        raise ValueError(f"[Backfill] Missing key columns for {full_table_name}: {missing_keys}")

    backfill_df = cast_columns_to_target_schema(backfill_df, target_schema_map)
    sequence_column_lc = (sequence_column or "").strip().lower()

    update_pairs = []
    for source_col_lc, source_col in source_columns.items():
        if source_col_lc in PIPELINE_SYSTEM_COLUMNS:
            continue
        if source_col_lc == sequence_column_lc:
            continue
        if source_col_lc in {pk.lower() for pk in normalized_primary_keys}:
            continue
        if source_col_lc not in target_columns:
            continue
        update_pairs.append((target_columns[source_col_lc], source_col))

    if not update_pairs:
        print(f"[Backfill] No eligible nullable columns found for {full_table_name}.")
        return

    backfill_df.createOrReplaceTempView("_backfill_src")

    join_condition = " AND ".join([
        f"t.{quote_identifier(target_columns[pk.lower()])} = s.{quote_identifier(source_columns[pk.lower()])}"
        for pk in normalized_primary_keys
    ])
    update_set = ", ".join([
        f"t.{quote_identifier(target_col)} = CASE WHEN t.{quote_identifier(target_col)} IS NULL THEN s.{quote_identifier(source_col)} ELSE t.{quote_identifier(target_col)} END"
        for target_col, source_col in update_pairs
    ])

    spark.sql(f"""
        MERGE INTO {quote_table_name(full_table_name)} AS t
        USING _backfill_src AS s
        ON {join_condition}
        WHEN MATCHED THEN UPDATE SET {update_set}
    """)

    spark.sql(f"""
        UPDATE {quote_table_name(audit_table)}
        SET backfill_completion_status = 'COMPLETED'
        WHERE lower(table_name) = lower('{audit_table_name}')
          AND lower(schema_drift_id) = lower('{schema_drift_id}')
          AND backfill_completion_status = 'PENDING'
    """)

    print(f"[Backfill] Updated {full_table_name} from {parquet_path}.")
    print(f"[Backfill] Marked audit row COMPLETED for schema_drift_id={schema_drift_id}.")

# COMMAND ----------

# DBTITLE 1,Metadata Fetch from SchemaEvolutionQueue and Pipeline Execution
# ── Fetch metadata from SchemaEvolutionQueue and execute backfill ─────────────────

url, user, password, driver = dbutils.secrets.get(v_sql_server, v_sql_secret).split("|")
queue_table = "[dbo].[SchemaEvolutionQueue]"

schema_df = (
    spark.read.format("jdbc")
    .option("url", url)
    .option("query", f"SELECT TOP 0 * FROM {queue_table}")
    .option("user", user)
    .option("password", password)
    .option("driver", driver)
    .load()
)
queue_columns = {col.lower(): col for col in schema_df.columns}

adf_job_column = next((queue_columns[col] for col in ["adf_job_name", "adfjobname"] if col in queue_columns), None)
target_catalog_column = next((queue_columns[col] for col in ["targetcatalogname", "catalogname", "destinationcatalogname"] if col in queue_columns), None)
target_schema_column = next((queue_columns[col] for col in ["targetschemasname", "targetschemaname", "destinationschemaname"] if col in queue_columns), None)
target_table_column = next((queue_columns[col] for col in ["targettablename", "destinationtablename"] if col in queue_columns), None)

escaped_drift_id = drift_id.replace("'", "''")
escaped_adf_job_name = adf_job_name.replace("'", "''")

filters = [f"lower(schema_drift_id) = lower('{escaped_drift_id}')"]
if adf_job_name and adf_job_column:
    filters.append(f"lower({adf_job_column}) = lower('{escaped_adf_job_name}')")
elif adf_job_name:
    print("[Metadata] adf_job_name provided, but no matching JDBC column was found. Filtering only by schema_drift_id.")

metadata_query = f"""
    SELECT *
    FROM {queue_table}
    WHERE {' AND '.join(filters)}
"""
print(f"[Metadata] Fetching backfill records for schema_drift_id={drift_id}")

metadata_df = (
    spark.read.format("jdbc")
    .option("url", url)
    .option("query", metadata_query)
    .option("user", user)
    .option("password", password)
    .option("driver", driver)
    .load()
)
metadata_rows = metadata_df.collect()

if not metadata_rows:
    raise ValueError(f"[Backfill] No records found in {queue_table} for schema_drift_id='{drift_id}'.")

print(f"[Backfill] Found {len(metadata_rows)} table(s) to process.")

for row in metadata_rows:
    folder_path = str(row["FolderPath"]).strip().lower()
    source_db_name = str(row["TargetDBName"]).strip().lower()
    source_schema_name = str(row["TargetSchemaName"]).strip().lower()
    source_table_name = str(row["TargetTableName"]).strip().lower()
    primary_keys = [x.strip() for x in str(row["DeltaColumnName"]).split(",") if x and x.strip()]
    sequence_column = str(row["WaterMarkColumnName"]).strip()

    parquet_path = f"{volume_path}/{source_db_name}/{source_schema_name}/{source_table_name}/"

    resolved_catalog = str(row[target_catalog_column]).strip() if target_catalog_column and row[target_catalog_column] else audit_catalog
    resolved_schema = str(row["TargetDBName"]).strip().lower() if row["TargetDBName"] else "finsight"
    resolved_table = str(row[target_table_column]).strip() if target_table_column and row[target_table_column] else folder_path
    full_table_name = f"{resolved_catalog}.{resolved_schema}.{resolved_table}"

    print(f"[Backfill] Processing drift_id={drift_id}, target={full_table_name}, parquet_path={parquet_path}")
    run_backfill_merge(
        parquet_path=parquet_path,
        full_table_name=full_table_name,
        primary_keys=primary_keys,
        sequence_column=sequence_column,
        audit_table=SCHEMA_AUDIT_TABLE,
        schema_drift_id=drift_id,
        audit_table_name=folder_path
    )