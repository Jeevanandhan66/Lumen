# Databricks notebook source
import dlt

from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, LongType, TimestampType, DateType
import builtins
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession


# COMMAND ----------

# DBTITLE 1,Cell 2

adf_job_name = spark.conf.get("mypipeline.adf_job_name")

query = """
        SELECT distinct catalog_name FROM system.information_schema.catalogs WHERE catalog_name like '%landing_%' order by catalog_name
        """
result = spark.sql(query).collect()
cluster_all_tags_str = spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags")
cluster_all_tags = json.loads(cluster_all_tags_str)
env_tag = next((tag for tag in cluster_all_tags if tag.get('key') == 'environment'), {}).get('value')

# Dynamically fetch the destination catalog from DLT pipeline settings
# This allows the same notebook to target any catalog (udl, asl, etc.) without code changes
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
        break  # Exit the loop if "landing" is found exactly
    elif 'dev_landing_dlh_finance' == catalog_name and env_tag in catalog_name:
        v_landing_catalog = 'dev_landing_dlh_finance'
        v_sql_server = 'sql-server-dlhfinance'
        v_sql_secret = 'sql-server-dlhfinance-dev'

# Check if v_landing_catalog was assigned a value
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
# Audit table lives in the same catalog as the pipeline target so prod/dev are isolated
_audit_catalog = catalog_name_value if catalog_name_value else "dev_udl_dlh_finance"
SCHEMA_AUDIT_TABLE = f"{_audit_catalog}.finsight.finsight_schema_audit"

# Columns injected by the pipeline itself — excluded from drift comparison
PIPELINE_SYSTEM_COLUMNS = {
    "DBX_LOAD_DT", "__START_AT", "__END_AT",
    "_COMMIT_VERSION", "_COMMIT_TIMESTAMP", "__DELETED",
    "_RESCUED_DATA"   # DLT schema-evolution rescue column
}
# ─────────────────────────────────────────────────────────────────────────────────────


def log_schema_audit(table_name, adfjobname, columns_added, columns_removed):
    """
    Append one audit record to finsight_schema_audit.
    Called for every table check — drift or no-drift.

    backfill_status            = True   when new columns are added (historical rows lack them)
    backfill_completion_status = PENDING      when backfill is needed
                                 NOT_REQUIRED when schema is stable or only columns removed
    """
    from datetime import datetime

    has_additions = len(columns_added) > 0

    audit_schema = StructType([
        StructField("table_name",                 StringType(),            True),
        StructField("adfjobname",                 StringType(),            True),
        StructField("columns_removed",            ArrayType(StringType()), True),
        StructField("columns_added",              ArrayType(StringType()), True),
        StructField("backfill_status",            BooleanType(),           True),
        StructField("backfill_completion_status", StringType(),            True),
        StructField("audit_timestamp",            TimestampType(),         True),
    ])

    audit_row = [(
        table_name,
        adfjobname,
        columns_removed,
        columns_added,
        has_additions,                                       # True only when columns are added
        "PENDING" if has_additions else "NOT_REQUIRED",
        datetime.now()
    )]

    spark.createDataFrame(audit_row, schema=audit_schema) \
         .write.format("delta").mode("append").saveAsTable(SCHEMA_AUDIT_TABLE)

    # Console summary
    if not columns_added and not columns_removed:
        print(f"[Schema Audit] {table_name}: No drift detected.")
    else:
        if columns_added:
            print(f"[Schema Audit] {table_name}: Columns ADDED   → {columns_added}")
        if columns_removed:
            print(f"[Schema Audit] {table_name}: Columns REMOVED → {columns_removed}")


def detect_and_log_schema_drift(folder_path, data_path, full_table_name):
    """
    Compare incoming parquet columns against the existing target table schema
    and log the result to finsight_schema_audit. Covers all four cases:
      - No drift              → log empty lists, NOT_REQUIRED
      - Columns added only    → log added list,  backfill PENDING
      - Columns removed only  → log removed list, NOT_REQUIRED
      - Both added & removed  → log both lists,   backfill PENDING
    First-time load (table does not yet exist) → all incoming columns treated as added.
    """
    try:
        sample_df     = spark.read.format("parquet").load(data_path)
        incoming_cols = {c.upper() for c in sample_df.columns} - PIPELINE_SYSTEM_COLUMNS

        if table_exists(full_table_name):
            schema_rows   = spark.sql(f"DESCRIBE TABLE {full_table_name}").collect()
            existing_cols = {
                row['col_name'].upper()
                for row in schema_rows
                if row['col_name'] and not row['col_name'].startswith('#')
            } - PIPELINE_SYSTEM_COLUMNS

            columns_added   = sorted(incoming_cols - existing_cols)
            columns_removed = sorted(existing_cols - incoming_cols)
        else:
            # First-time table creation — all incoming columns are new
            columns_added   = sorted(incoming_cols)
            columns_removed = []

        log_schema_audit(
            table_name=folder_path,
            adfjobname=adf_job_name,
            columns_added=columns_added,
            columns_removed=columns_removed
        )

    except Exception as e:
        print(f"[Schema Audit] WARNING: Drift check failed for {folder_path}: {e}")


def cast_decimal_columns(df, oracle_schema_map=None):
    """
    Cast columns to the SQL types provided in oracle_schema_map.
    """
    if not oracle_schema_map:
        return df

    for col_name, sql_type in oracle_schema_map.items():
        if col_name in df.columns:

            # Fix DATE columns with MM/dd/yyyy format
            if sql_type == "DATE":
                df = df.withColumn(col_name, F.to_date(F.col(col_name), "MM/dd/yyyy"))

            else:
                df = df.withColumn(col_name, F.col(col_name).cast(sql_type))

    return df

 

def compute_precision_scale(batch_df=None, mode="data", table_name=None, max_scale=2):
    """
    Infer SQL types for numeric columns.
 
    Parameters
    ----------
    batch_df   : DataFrame
        Static DataFrame to inspect when mode='data'.
    mode       : str
        'data'  -> derive schema from the provided batch_df
        'table' -> read schema directly from an existing Unity Catalog table
    table_name : str
        Fully qualified table name (e.g. 'my_catalog.my_schema.my_table')
        Required when mode='table'.
    max_scale  : int
        Cap fractional digits to this value when inferring from data.
 
    Returns
    -------
    dict
        { column_name : "INT" | "BIGINT" | "DECIMAL(p,s)" }
    """
    if mode not in ("data", "table"):
        raise ValueError("mode must be 'data' or 'table'")
 
    result = {}
 
    # ----------------------------------------------------------------------
    # 1️⃣  Infer from Data Sample
    # ----------------------------------------------------------------------
    if mode == "data":
        for field in batch_df.schema.fields:
            col = field.name
            dtype = field.dataType

            # Integer-like
            if isinstance(dtype, (IntegerType, LongType)):
                result[col] = "BIGINT"

            # Numeric types: Decimal, Double, Float
            elif isinstance(dtype, (DecimalType, DoubleType, FloatType)):

                # Sample values to inspect fractional behavior
                sample_vals = (
                    batch_df.select(col)
                    .dropna()
                    .limit(200)
                    .toPandas()[col]
                    .astype(float)
                )

                # Convert to Python list
                vals = [float(v) for v in sample_vals]
                if not vals:
                    # If all values are null, default to DECIMAL(38,2)
                    result[col] = f"DECIMAL(38,{max_scale})"
                else:
                    # Check if all values have zero fractional part
                    all_whole_numbers = all(v.is_integer() for v in vals)

                    if all_whole_numbers:
                        # Determine INT vs BIGINT based on magnitude
                        abs_vals = [builtins.abs(v) for v in vals]   # <-- FIXED
                        if abs_vals:  # Only call max() if the list is not empty
                            max_val = builtins.max(abs_vals)           # <-- FIX

                            if max_val <= 2_147_483_647:
                                result[col] = "BIGINT"
                            else:
                                result[col] = "BIGINT"

                    else:
                        # True decimals → DECIMAL(38, max_scale)
                        result[col] = f"DECIMAL(38,{max_scale})"

            # Timestamp
            elif isinstance(dtype, TimestampType):
                result[col] = "TIMESTAMP"

            # Date
            elif isinstance(dtype, DateType):
                result[col] = "DATE"

            # String → try detect date
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
            
    # ----------------------------------------------------------------------
    # 2️⃣  Infer from Existing Unity Catalog Table (metadata only)
    # ----------------------------------------------------------------------
    else:  # mode == "table"
        if not table_name:
            raise ValueError("table_name is required when mode='table'.")
 
        # Use DESCRIBE TABLE to fetch schema metadata (avoids spark.table in DLT)
        schema_df = spark.sql(f"DESCRIBE TABLE {table_name}")
 
        for row in schema_df.collect():
            col_name  = row['col_name']
            data_type = (row['data_type'] or "").upper()
 
            if not col_name or col_name.startswith("#"):  # skip metadata rows
                continue
 
            if data_type == "INT":
                result[col_name] = "BIGINT"
            elif data_type == "BIGINT":
                result[col_name] = "BIGINT"
            if data_type in ("INT", "INTEGER", "BIGINT", "LONG"):
                result[col_name] = "BIGINT"
            elif data_type.startswith("DECIMAL"):
                result[col_name] = f"DECIMAL(38,{max_scale})"  # already DECIMAL(p,s)
            elif data_type == "DATE":
                result[col_name] = "DATE"
            elif data_type == "TIMESTAMP":
                result[col_name] = "TIMESTAMP"
            elif data_type in ("DOUBLE", "FLOAT", "REAL"):
                result[col_name] = "DOUBLE"
            elif data_type.startswith("SMALLINT") or data_type.startswith("TINYINT"):
                result[col_name] = "INT"  # promote to INT
            else:
                result[col_name] = "STRING"
 
    return result
def table_exists(full_table_name: str) -> bool:
    """
    Check table existence by attempting DESCRIBE TABLE directly.
    More reliable than SHOW TABLES IN DLT pipeline planning context
    where streaming tables may not appear in catalog listings.
    """
    try:
        spark.sql(f"DESCRIBE TABLE {full_table_name}")
        return True
    except Exception:
        return False

def ingest_streaming_table(databricks_table_name, source_db_name, schema_name, FolderPath, Pkeys, scd_typ, sequence_column):

    # Resolve paths once — shared by drift detection and the DLT bronze table
    data_path       = f"{volume_path}/{source_db_name}/{schema_name}/{databricks_table_name}/"
    full_table_name = f"{catalog_name_value}.finsight.{FolderPath}"

    # ── Schema drift detection (runs at DLT pipeline planning time) ────────
    detect_and_log_schema_drift(FolderPath, data_path, full_table_name)
    # ────────────────────────────────────────────────────────────────────────

    @dlt.table(
        name=f"{FolderPath}_cdc_raw",
        table_properties={ "quality": "bronze" }
    )
    def create_table():
        sample_df = spark.read.format("parquet").load(data_path)

        # Use existing table schema if available, otherwise infer from sample
        if table_exists(full_table_name):
            schema_map = compute_precision_scale(mode="table", table_name=full_table_name)
        else:
            schema_map = compute_precision_scale(batch_df=sample_df, mode="data")
        
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

    # ✅ Define the Silver table with CDC merge logic
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

query = f"""SELECT 
                lower(TargetTableName) as TargetTableName,
                lower(TargetDBName) as TargetDBName,
                lower(TargetSchemaName) as TargetSchemaName,  lower(FolderPath) as FolderPath,
                lower(DeltaColumnName) as DeltaColumnName,
                DBXSCDType,
                lower(WaterMarkColumnName) as WaterMarkColumnName
                FROM [dbo].[ControlTableIntegrated] 
                WHERE lower(AdfJobName) = '{adf_job_name}'
                --AND CopyMode = 'INCREMENTAL' 
                AND IsActive = 'Y' 
                 """ 
    
url, user, password, driver = dbutils.secrets.get(v_sql_server, v_sql_secret).split("|")
    
metadata_df = spark.read.format("jdbc") \
        .option("url", url) \
        .option("query", query) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver) \
        .load()

for row in metadata_df.collect():  
    #ingest_streaming_table(row['TargetTableName'],row['TargetDBName'],row['TargetSchemaName'],row['DeltaColumnName'],row['DBXSCDType'], row['WaterMarkColumnName'])
    pkeys = row['DeltaColumnName']
    if ',' in pkeys:
        pkeys = [x.strip() for x in pkeys.split(',')]  # convert comma list to array
    ingest_streaming_table(
        row['TargetTableName'],
        row['TargetDBName'],
        row['TargetSchemaName'], row['FolderPath'] ,
        pkeys,                               # <-- pass as list
        row['DBXSCDType'],
        row['WaterMarkColumnName']
    )
    #display(row)