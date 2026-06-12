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

# DBTITLE 1,[Not Needed in Prod] Delete S3 Locations to Start Fresh Test
dbutils.fs.rm(f"{s3_prefix}/table1", recurse=True)
dbutils.fs.rm(f"{s3_prefix}/table2", recurse=True)
dbutils.fs.rm(f"{s3_prefix}/joined", recurse=True)
dbutils.fs.rm(f"{s3_prefix}/checkpoint_join1", recurse=True)
dbutils.fs.rm(f"{s3_prefix}/checkpoint_join2", recurse=True)

# COMMAND ----------

# DBTITLE 1,Create Tables
create_tables(input)

# COMMAND ----------

# DBTITLE 1,[Not Needed in Prod] Synthetic Dataframe for Table 1
df1 = (spark.range(param['test_size'])
    .withColumn("table1", F.lit(1))
    .withColumn("batch1", F.lit(1))
    .withColumn("random1", F.expr("uuid()"))
)

display(df1.where(f"{input['join_col']} >= {param['display_start']}").orderBy(input['join_col']))

# COMMAND ----------

# DBTITLE 1,[Not Needed in Prod] Synthetic Dataframe for Table 2
df2 = (spark.range(param['test_size'])
    .withColumn("table2", F.lit(2))
    .withColumn("batch2", F.lit(1))
    .withColumn("random2", F.expr("uuid()"))
)

display(df2.where(f"{input['join_col']} >= {param['display_start']}").orderBy(input['join_col']))

# COMMAND ----------

# DBTITLE 1,[Replace with Full Load] Write Dataframe to S3 Table 1
df1.write.saveAsTable(f"{input['db_name']}.table1", mode="overwrite")

# COMMAND ----------

# DBTITLE 1,[Replace with Optimization Chosen e.g. Liquid Clustering] Optimize with Zorder Table 2
display(spark.sql(f"OPTIMIZE {input['db_name']}.table1 ZORDER BY {input['join_col']}"))

# COMMAND ----------

# DBTITLE 1,[Replace with Full Load] Write Dataframe to S3 Table 2
df2.write.saveAsTable(f"{input['db_name']}.table2", mode="overwrite")

# COMMAND ----------

# DBTITLE 1,[Replace with Optimization Chosen e.g. Liquid Clustering] Optimize with Zorder Table 2
display(spark.sql(f"OPTIMIZE {input['db_name']}.table2 ZORDER BY {input['join_col']}"))

# COMMAND ----------

# DBTITLE 1,Perform Static Join of Full Load Tables
table1_df = spark.read.load(f"{s3_prefix}/table1")
table2_df = spark.read.load(f"{s3_prefix}/table2")

(table1_df
    .join(table2_df, "id", "inner")
    .write
    .save(f"{s3_prefix}/joined")
)

# COMMAND ----------

# DBTITLE 1,[Not Needed in Prod] View Joined Data Sample
display(spark.read.load(f"{s3_prefix}/joined").where(f"id >= {param['display_start']}").orderBy("id"))

# COMMAND ----------

# DBTITLE 1,Turn on CDF After Full Load (Creates New Version in History)
spark.sql(f"ALTER TABLE {input['db_name']}.table1 SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
spark.sql(f"ALTER TABLE {input['db_name']}.table2 SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
