# Databricks notebook source
# DBTITLE 1,Imports and Drift ID
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, LongType, TimestampType, DateType
import builtins
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession
from uuid import uuid4

# Generate a new schema_drift_id for each incremental run
drift_id = str(uuid4())
print(f"[Pipeline] Generated new schema_drift_id for this run: {drift_id}")

# COMMAND ----------

# DBTITLE 1,SchemaEvolutionQueue Entry
def seqEntry(columns_added):
    """
    Inserts one record per ControlTableIntegrated row into [dbo].[SchemaEvolutionQueue].
    Called from log_schema_audit only when new columns are detected (len(columns_added) > 0).
    Reuses metadata_rows (collected in the metadata cell) and drift_id (generated in cell 1).
    """
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    print(f"[SchemaEvolutionQueue] Using schema_drift_id: {drift_id}")

    seq_schema = StructType([
        StructField("AdfJobName",           StringType(),  True),
        StructField("TableID",              IntegerType(), True),
        StructField("schema_drift_id",      StringType(),  True),
        StructField("SourceSchemaName",     StringType(),  True),
        StructField("SourceTableName",      StringType(),  True),
        StructField("SourceID",             IntegerType(), True),
        StructField("SourceQuery",          StringType(),  True),
        StructField("TargetTableName",      StringType(),  True),
        StructField("TargetSchemaName",     StringType(),  True),
        StructField("TargetDBName",         StringType(),  True),
        StructField("DBXSCDType",           StringType(),  True),
        StructField("DBXPipelineId",        StringType(),  True),
        StructField("JobId",                StringType(),  True),
        StructField("ADLSContainerName",    StringType(),  True),
        StructField("CopyMode",             StringType(),  True),
        StructField("DeltaColumnName",      StringType(),  True),
        StructField("IsActive",             StringType(),  True),
        StructField("WaterMarkColumnName",  StringType(),  True),
        StructField("WaterMarkColumnType",  StringType(),  True),
        StructField("FolderPath",           StringType(),  True),
        StructField("PartitionColumnName",  StringType(),  True),
        StructField("PartitionLowerBound",  StringType(),  True),
        StructField("PartitionUpperBound",  StringType(),  True),
        StructField("MaskingSchemas",       StringType(),  True),
        StructField("IndField",             StringType(),  True),
        StructField("IndValue",             StringType(),  True),
    ])

    seq_rows = [
        (
            f"{row['AdfJobName']}_backfill",
            int(row["TableID"])  if row["TableID"]  is not None else None,
            drift_id,
            row["SourceSchemaName"],
            row["SourceTableName"],
            int(row["SourceID"]) if row["SourceID"] is not None else None,
            f"SELECT {row['DeltaColumnName']}, {row['WaterMarkColumnName']}, {', '.join(columns_added)} FROM ({row['SourceQuery']}) A ",
            row["TargetTableName"],
            row["TargetSchemaName"],
            row["TargetDBName"],
            row["DBXSCDType"],
            row["DBXPipelineId"],
            row["JobId"],
            row["ADLSContainerName"],
            row["CopyMode"],
            row["DeltaColumnName"],
            row["IsActive"],
            row["WaterMarkColumnName"],
            row["WaterMarkColumnType"],
            row["FolderPath"],
            row["PartitionColumnName"],
            row["PartitionLowerBound"],
            row["PartitionUpperBound"],
            row["MaskingSchemas"],
            row["IndField"],
            row["IndValue"],
        )
        for row in metadata_rows
    ]

    seq_df = spark.createDataFrame(seq_rows, schema=seq_schema)

    seq_df.write.format("jdbc") \
        .option("url",      url) \
        .option("dbtable",  "[dbo].[SchemaEvolutionQueue]") \
        .option("user",     user) \
        .option("password", password) \
        .option("driver",   driver) \
        .mode("append") \
        .save()

    print(f"[SchemaEvolutionQueue] Inserted {len(seq_rows)} record(s) into [dbo].[SchemaEvolutionQueue]")

# COMMAND ----------

# DBTITLE 1,Environment Config and Utility Functions
adf_job_name = spark.conf.get("mypipeline.adf_job_name")

query = """
        SELECT distinct catalog_name FROM system.information_schema.catalogs WHERE catalog_name like '%landing_%' order by catalog_name
        """
result = spark.sql(query).collect()
cluster_all_tags_str = spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags")
cluster_all_tags = json.loads(cluster_all_tags_str)
env_tag = next((tag for tag in cluster_all_tags if tag.get('key') == 'environment'), {}).get('value')

# Dynamically fetch the destination catalog from DLT pipeline settings
catalog_name_value = spark.conf.get("pipelines.target.catalog", None)

# Initialize v_landing_catalog with a default value
v_landing_catalog = None
v_sql_server = None
v_sql_secret = None

# Iterate through the result and check for the desired words
for row in result:
    catalog_name = row['catalog_name']
    if 'landing_dlh_finance' == catalog_name:
        v_landing_catalog = 'landing_dlh_finance'
        v_sql_server = 'dlhfinance-prod-scope'
        v_sql_secret = 'sql-server-dlhfinance-prod'
        break
    elif 'dev_landing_dlh_finance' == catalog_name and env_tag in catalog_name:
        v_landing_catalog = 'dev_landing_dlh_finance'
        v_sql_server = 'sql-server-dlhfinance'
        v_sql_secret = 'sql-server-dlhfinance-dev'

if v_landing_catalog is None:
    print("No matching catalog found.")
else:
    print(f"Selected catalog: {v_landing_catalog}")

if catalog_name_value is None:
    print("Warning: pipelines.target.catalog not set. Schema lookup will fall back to data inference.")
else:
    print(f"Destination catalog: {catalog_name_value}")

volume_path = f'/Volumes/{v_landing_catalog}'

# ── Schema Audit Configuration ────────────────────────────────────────────────────────
_audit_catalog = catalog_name_value if catalog_name_value else "dev_udl_dlh_finance"
SCHEMA_AUDIT_TABLE = f"{_audit_catalog}.finsight.finsight_schema_audit"

# Columns injected by the pipeline itself — excluded from drift comparison
PIPELINE_SYSTEM_COLUMNS = {
    "DBX_LOAD_DT", "__START_AT", "__END_AT",
    "_COMMIT_VERSION", "_COMMIT_TIMESTAMP", "__DELETED",
    "_RESCUED_DATA"
}
# ─────────────────────────────────────────────────────────────────────────────────────


def log_schema_audit(table_name, adfjobname, columns_added, columns_removed):
    """
    Append one audit record to finsight_schema_audit.
    backfill_status            = True   when new columns are added
    backfill_completion_status = PENDING when backfill is needed, NOT_REQUIRED otherwise
    """
    from datetime import datetime

    has_additions = len(columns_added) > 0

    audit_schema = StructType([
        StructField("schema_drift_id",            StringType(),            True),
        StructField("table_name",                 StringType(),            True),
        StructField("adfjobname",                 StringType(),            True),
        StructField("columns_removed",            ArrayType(StringType()), True),
        StructField("columns_added",              ArrayType(StringType()), True),
        StructField("backfill_status",            BooleanType(),           True),
        StructField("backfill_completion_status", StringType(),            True),
        StructField("audit_timestamp",            TimestampType(),         True),
    ])

    audit_row = [(
        drift_id,
        table_name,
        adfjobname,
        columns_removed,
        columns_added,
        has_additions,
        "PENDING" if has_additions else "NOT_REQUIRED",
        datetime.now()
    )]

    spark.createDataFrame(audit_row, schema=audit_schema) \
         .write.format("delta").mode("append").saveAsTable(SCHEMA_AUDIT_TABLE)

    # Trigger SchemaEvolutionQueue entry only when new columns are detected
    if len(columns_added) > 0:
        seqEntry(columns_added)


def detect_and_log_schema_drift(folder_path, data_path, full_table_name):
    """
    Compare incoming parquet columns against the existing target table schema
    and log the result to finsight_schema_audit.
    """
    try:
        sample_df     = spark.read.format("parquet").load(data_path)
        incoming_cols = set([c.upper() for c in sample_df.columns]) - PIPELINE_SYSTEM_COLUMNS

        if table_exists(full_table_name):
            catalog_p, schema_p, table_p = full_table_name.split(".")
            existing_cols = {
                row["column_name"].upper()
                for row in spark.sql(f"""
                    SELECT column_name
                    FROM {catalog_p}.information_schema.columns
                    WHERE table_schema = '{schema_p}' AND table_name = '{table_p}'
                """).collect()
            } - PIPELINE_SYSTEM_COLUMNS

            columns_added   = sorted([col for col in incoming_cols if col not in existing_cols])
            columns_removed = sorted([col for col in existing_cols if col not in incoming_cols])
        else:
            columns_added   = sorted(incoming_cols)
            columns_removed = []

        if columns_added or columns_removed:
            # Idempotency guard: skip if an unresolved record with identical drift already exists
            _added_literal   = ', '.join([f"'{c}'" for c in columns_added])   if columns_added   else ''
            _removed_literal = ', '.join([f"'{c}'" for c in columns_removed]) if columns_removed else ''

            existing_pending = spark.sql(f"""
                SELECT 1 FROM {SCHEMA_AUDIT_TABLE}
                WHERE table_name = '{folder_path}'
                  AND backfill_completion_status IN ('PENDING', 'NOT_REQUIRED')
                  AND sort_array(columns_added)   = sort_array(array({_added_literal}))
                  AND sort_array(columns_removed) = sort_array(array({_removed_literal}))
                LIMIT 1
            """).count()

            if existing_pending > 0:
                print(f"[Schema Audit] {folder_path}: PENDING record with same columns already exists — skipping duplicate write (Auto Loader retry).")
            else:
                log_schema_audit(
                    table_name=folder_path,
                    adfjobname=adf_job_name,
                    columns_added=columns_added,
                    columns_removed=columns_removed
                )
        else:
            print(f"[Schema Audit] {folder_path}: No drift detected — skipping audit write.")

    except Exception as e:
        print(f"[Schema Audit] WARNING: Drift check failed for {folder_path}: {e}")


def cast_decimal_columns(df, oracle_schema_map=None):
    """Cast columns to the SQL types provided in oracle_schema_map."""
    if not oracle_schema_map:
        return df

    for col_name, sql_type in oracle_schema_map.items():
        if col_name in df.columns:
            if sql_type == "DATE":
                df = df.withColumn(col_name, F.to_date(F.col(col_name), "MM/dd/yyyy"))
            else:
                df = df.withColumn(col_name, F.col(col_name).cast(sql_type))

    return df


def compute_precision_scale(batch_df=None, mode="data", table_name=None, max_scale=2, sequence_column=None):
    """
    Infer SQL types for numeric columns.
    Modes: 'data' (from DataFrame sample) or 'table' (from existing UC table metadata).
    """
    if mode not in ("data", "table"):
        raise ValueError("mode must be 'data' or 'table'")

    result = {}

    if mode == "data":
        for field in batch_df.schema.fields:
            col = field.name
            dtype = field.dataType

            if isinstance(dtype, (IntegerType, LongType)):
                result[col] = "BIGINT"
            elif isinstance(dtype, (DecimalType, DoubleType, FloatType)):
                sample_vals = (
                    batch_df.select(col)
                    .dropna()
                    .limit(200)
                    .toPandas()[col]
                    .astype(float)
                )
                vals = [float(v) for v in sample_vals]
                if not vals:
                    result[col] = f"DECIMAL(38,{max_scale})"
                else:
                    all_whole_numbers = all(v.is_integer() for v in vals)
                    if all_whole_numbers:
                        result[col] = "BIGINT"
                    else:
                        result[col] = f"DECIMAL(38,{max_scale})"
            elif isinstance(dtype, TimestampType):
                result[col] = "TIMESTAMP"
            elif isinstance(dtype, DateType):
                result[col] = "DATE"
            elif isinstance(dtype, StringType):
                sample = batch_df.select(col).dropna().limit(50).toPandas()[col].astype(str)
                if sample.str.match(r"\d{4}-\d{2}-\d{2}$").all():
                    result[col] = "DATE"
                elif sample.str.match(r"\d{2}/\d{2}/\d{4}$").all():
                    result[col] = "DATE"
                else:
                    result[col] = "STRING"
            else:
                result[col] = "STRING"

    else:  # mode == "table"
        if not table_name:
            raise ValueError("table_name is required when mode='table'.")

        _parts = table_name.split(".")
        catalog_p, schema_p, table_p = _parts[0], _parts[1], _parts[2]
        schema_df = spark.sql(f"""
            SELECT column_name AS col_name, data_type
            FROM {catalog_p}.information_schema.columns
            WHERE table_schema = '{schema_p}' AND table_name = '{table_p}'
        """)

        for row in schema_df.collect():
            col_name  = row['col_name']
            data_type = (row['data_type'] or "").upper()

            if data_type in ("INT", "INTEGER", "BIGINT", "LONG"):
                result[col_name] = "BIGINT"
            elif data_type.startswith("DECIMAL"):
                result[col_name] = f"DECIMAL(38,{max_scale})"
            elif data_type == "DATE":
                result[col_name] = "DATE"
            elif data_type == "TIMESTAMP":
                result[col_name] = "TIMESTAMP"
            elif data_type in ("DOUBLE", "FLOAT", "REAL"):
                result[col_name] = "DOUBLE"
            elif data_type.startswith("SMALLINT") or data_type.startswith("TINYINT"):
                result[col_name] = "INT"
            else:
                result[col_name] = "STRING"

    return result


def resolve_compute_mode(full_table_name: str) -> str:
    """
    Determines the correct mode for compute_precision_scale.
    For incremental: 'table' if target exists, else 'data' (first load).
    """
    if table_exists(full_table_name):
        return "table"
    else:
        return "data"


def table_exists(full_table_name: str) -> bool:
    """Check table existence via information_schema.tables."""
    try:
        parts = full_table_name.split(".")
        if len(parts) != 3:
            return False
        catalog_p, schema_p, table_p = parts
        result = spark.sql(f"""
            SELECT COUNT(*) AS cnt
            FROM {catalog_p}.information_schema.tables
            WHERE table_schema = '{schema_p}' AND table_name = '{table_p}'
        """).collect()
        return result[0]["cnt"] > 0
    except Exception:
        return False


def ingest_streaming_table(databricks_table_name, source_db_name, schema_name, FolderPath, Pkeys, scd_typ, sequence_column):

    # Resolve paths
    data_path       = f"{volume_path}/{source_db_name}/{schema_name}/{databricks_table_name}/"
    full_table_name = f"{_audit_catalog}.finsight.{FolderPath}"

    # ── Schema drift detection (runs only for existing tables) ────────
    if table_exists(full_table_name):
        detect_and_log_schema_drift(FolderPath, data_path, full_table_name)

    @dlt.table(
        name=f"{FolderPath}_cdc_raw",
        table_properties={ "quality": "bronze" }
    )
    def create_table():
        sample_df = spark.read.format("parquet").load(data_path)

        mode = resolve_compute_mode(full_table_name)
        schema_map = compute_precision_scale(batch_df=sample_df, mode=mode, table_name=full_table_name, sequence_column=sequence_column)

        df = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.inferSchema", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(data_path)
            .withColumn('DBX_LOAD_DT', to_timestamp(current_timestamp()))
        )
        df = cast_decimal_columns(df, schema_map)
        return df

    # Silver table with CDC merge logic
    dlt.create_streaming_table(
        name=f"{FolderPath}",
        comment="Silver Table with incremental data"
    )

    dlt.create_auto_cdc_flow(
        target=f"{FolderPath}",
        source=f"{FolderPath}_cdc_raw",
        keys=[Pkeys] if isinstance(Pkeys, str) else Pkeys,
        sequence_by=F.col(sequence_column),
        stored_as_scd_type=scd_typ,
    )

# COMMAND ----------

# DBTITLE 1,Metadata Fetch and Pipeline Execution
# ── Fetch metadata from ControlTableIntegrated (normal incremental ingestion) ────────

url, user, password, driver = dbutils.secrets.get(v_sql_server, v_sql_secret).split("|")

query = f"""
    SELECT *
    FROM [dbo].[ControlTableIntegrated] 
    WHERE lower(AdfJobName) = '{adf_job_name}'
    AND IsActive = 'Y'
"""
print(f"[Metadata] Fetching from ControlTableIntegrated with adf_job_name: {adf_job_name}")

metadata_df = spark.read.format("jdbc") \
        .option("url", url) \
        .option("query", query) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver) \
        .load()

metadata_rows = metadata_df.collect()

for row in metadata_rows:
    pkeys = [x.strip().lower() for x in row['DeltaColumnName'].split(',')]
    ingest_streaming_table(
        row['TargetTableName'].lower(),
        row['TargetDBName'].lower(),
        row['TargetSchemaName'].lower(),
        row['FolderPath'].lower(),
        pkeys,
        row['DBXSCDType'],
        row['WaterMarkColumnName'].lower()
    )