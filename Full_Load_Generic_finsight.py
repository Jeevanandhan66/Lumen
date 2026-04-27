# Databricks notebook source
import dlt
import json
import re
#from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, LongType, TimestampType, DateType, DoubleType, FloatType
import builtins
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession

# Get ADF job name from pipeline config
adf_job_name = spark.conf.get("mypipeline.adf_job_name")

# Query available catalogs
query_asl = """
    SELECT DISTINCT catalog_name 
    FROM system.information_schema.catalogs 
    WHERE catalog_name LIKE '%landing_%' 
    ORDER BY catalog_name
"""
result = spark.sql(query_asl).collect()

# Get cluster environment tag
cluster_all_tags_str = spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags")
cluster_all_tags = json.loads(cluster_all_tags_str)
env_tag = next((tag for tag in cluster_all_tags if tag.get('key') == 'environment'), {}).get('value')

# Initialize catalog and secrets
v_landing_catalog = None
v_sql_server = None
v_sql_secret = None
catalog_name_value = None

# Select catalog based on environment
for row in result:
    catalog_name = row['catalog_name']
    if catalog_name == 'landing_dlh_finance':
        v_landing_catalog = 'landing_dlh_finance'
        v_sql_server = 'dlhfinance-prod-scope'
        v_sql_secret = 'sql-server-dlhfinance-prod'
        catalog_name_value = 'udl_dlh_finance'
        break
    elif catalog_name == 'dev_landing_dlh_finance' and env_tag in catalog_name:
        v_landing_catalog = 'dev_landing_dlh_finance'
        v_sql_server = 'sql-server-dlhfinance'
        v_sql_secret = 'sql-server-dlhfinance-dev'
        catalog_name_value = 'dev_udl_dlh_finance'

# Validate catalog selection
if v_landing_catalog is None:
    print("No matching catalog found.")
else:
    print(f"Selected catalog: {v_landing_catalog}")

# Construct volume path
volume_path = f'/Volumes/{v_landing_catalog}'


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
            
            elif isinstance(dtype, (DecimalType, DoubleType, FloatType)):

                sample_vals = (
                    batch_df.select(col)
                    .dropna()
                    .limit(200)
                    .toPandas()[col]
                    .astype(float)
                )

                vals = [float(v) for v in sample_vals]

                # If no numeric values exist, default to DECIMAL
                if len(vals) == 0:
                    result[col] = f"DECIMAL(38,{max_scale})"
                    continue

                all_whole_numbers = all(v.is_integer() for v in vals)

                if all_whole_numbers:                    
                    abs_vals = [builtins.abs(v) for v in vals]

                    # Guard again
                    if len(abs_vals) == 0:
                        result[col] = "BIGINT"
                        continue
                    
                    max_val = builtins.max(abs_vals)

                    result[col] = "BIGINT"

                else:
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
    try:
        catalog, schema, table = full_table_name.split(".")
        result = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").filter(f"tableName = '{table}'")
        return result.count() > 0
    except Exception as e:
        return False


# 📦 DLT table creation function
def full_copy_mv(target_table_name, target_db_name, target_schema_name, expectations):
    @dlt.table(
        name=target_table_name,
        table_properties={"quality": "bronze"},
        temporary=False
    )
    @dlt.expect_all(json.loads(expectations))
    def create_table():
        file_path = f"{volume_path}/{target_db_name}/{target_schema_name}/{target_table_name}/"
        #df = spark.read.format("parquet").load(file_path)
        #sample_df = spark.read.format("parquet").load(file_path) 
        sample_df = spark.read.option("mergeSchema", "true").parquet(file_path)
        full_table_name = f"{catalog_name_value}.ra_voice.{target_table_name}"

        # Check if the table exists
        if table_exists(full_table_name):
            schema_map = compute_precision_scale(mode="table", table_name=full_table_name)
        else:
            schema_map = compute_precision_scale(batch_df=sample_df, mode="data")

        # Detect and fix high-precision decimals        
        df = cast_decimal_columns(sample_df, schema_map)

        # Add load timestamp
        return df.withColumn('DBX_LOAD_DT', to_timestamp(current_timestamp()))

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
#supplier_invoice_expectations = '{"pk_nullcheck" : "SUPPLIER_INVOICE_ODS_ID IS NOT NULL"}'
# note that below the table names are lower cased in the metadata query because there is a case mismatch between
# the metadata table and the ADLS landing zone for the cods_finance schema
query = f"""SELECT 
                lower(TargetTableName) as TargetTableName,
                lower(TargetDBName) as TargetDBName,
                TargetSchemaName, 
                '{{}}' AS expectations
                FROM [dbo].[ControlTableIntegrated] 
                WHERE lower(AdfJobName) = '{adf_job_name}'
                AND CopyMode = 'FULL' 
                AND IsActive = 'Y' """  ## exclude tables with no data in ADLS
    
url, user, password, driver = dbutils.secrets.get(v_sql_server, v_sql_secret).split("|")
    
metadata_df = spark.read.format("jdbc") \
        .option("url", url) \
        .option("query", query) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver) \
        .load()

for row in metadata_df.collect():
        full_copy_mv(row['TargetTableName'],row['TargetDBName'],row['TargetSchemaName'],row['expectations'])
        #display(row)
