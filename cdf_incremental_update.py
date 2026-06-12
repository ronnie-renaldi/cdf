# Databricks notebook source
# DBTITLE 1,Data Bucket Location
log_dest = spark.conf.get("spark.databricks.clusterUsageTags.clusterLogDestination")
bucket_prefix = (log_dest
    .split('/')[2]
    .rstrip('-log')
)

s3_bucket = f"s3a://{bucket_prefix}-data"
print(f"{s3_bucket = }")
s3_prefix = f"{s3_bucket}/cmdp/test_join"
print(f"{s3_prefix = }")

# COMMAND ----------

# DBTITLE 1,[Redesign for Prod, Some for Synthetic Only] Defaults for Widget Inputs
input_defaults = {
    "test_size": "10000000",
    "test_ratio": "10000",
    "test_offset": "-1",
    "starting_version": "2",
    "batch": "2",
    "db_name": "cdf_test",
    "inc_table": "1",
    "join_table": "2",
    "join_col": "id",
}

# COMMAND ----------

# DBTITLE 1,[Not Needed in Prod] Define Ranges For Outputs and Synthetic Updates
def derived_param(input):
    test_size = int(input["test_size"])
    test_ratio = int(input["test_ratio"])
    test_offset = int(input["test_offset"])

    param = {}
    param['test_size'] = test_size
    param['display_start'] = (test_ratio - 2)*test_size//test_ratio
    param['update_start'] = (test_ratio + test_offset)*test_size//test_ratio
    param['update_end'] = (test_ratio + test_offset + 2)*test_size//test_ratio
    return param

# COMMAND ----------

# DBTITLE 1,Function to Create and Get Input Widgets
def get_widgets(input_defaults, post_action=derived_param):
    input = {}
    for key, default in input_defaults.items():
        dbutils.widgets.text(key, default)
        input[key] = dbutils.widgets.get(key)
    param = post_action(input)
    return input, param

# COMMAND ----------

# DBTITLE 1,[Do As Part of Full Load] Create Tables as Needed
def create_tables(input):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {input['db_name']} LOCATION '{s3_prefix}'")
    # spark.sql(f"CREATE TABLE IF NOT EXISTS {input['db_name']}.table1 LOCATION '{s3_prefix}/table1'")
    # spark.sql(f"CREATE TABLE IF NOT EXISTS {input['db_name']}.table2 LOCATION '{s3_prefix}/table2'")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {input['db_name']}.joined LOCATION '{s3_prefix}/joined'")

# COMMAND ----------

# DBTITLE 1,Imports
from delta.tables import *
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Create and Get Widget Params
input, param = get_widgets(input_defaults)
print(f"{input = }")
print(f"{param = }")

# COMMAND ----------

# DBTITLE 1,[Replace with Incremental Load] Create Synthetic Incremental Update
df_update = (spark
    .range(param['update_start'], param['update_end'])
    .withColumn(f"table{input['inc_table']}", F.lit(input['inc_table']))
    .withColumn(f"batch{input['inc_table']}", F.lit(input['batch']))
    .withColumn(f"random{input['inc_table']}", F.expr("uuid()"))
)

display(df_update)

# COMMAND ----------

# DBTITLE 1,Merge Update Into Table
deltaTable = DeltaTable.forPath(spark, f"{s3_prefix}/table{input['inc_table']}")

(deltaTable.alias("target")
    .merge(df_update.alias("update"), f"target.{input['join_col']} = update.{input['join_col']}")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

# DBTITLE 1,[Not Needed in Prod] Display Updates with PySpark
display(spark.read.load(f"{s3_prefix}/table{input['inc_table']}").where(f"{input['join_col']} >= {param['display_start']}").orderBy(input['join_col']))

# COMMAND ----------

# DBTITLE 1,[Not Needed in Prod] Display Updates with Spark SQL
display(spark.sql(f"SELECT * FROM {input['db_name']}.table{input['inc_table']} WHERE {input['join_col']} >= {param['display_start']} ORDER BY {input['join_col']}"))

# COMMAND ----------

# DBTITLE 1,[Not Needed in Prod] Display Table History - Note Version 1 Enabling Change Data Feed
display(spark.sql(f"DESCRIBE HISTORY cdf_test.table{input['inc_table']}"))
