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

# DBTITLE 1,Set up Stream-Static Join with Conditional Definitions, Depending on Which Table is Updated - Could be Function Calls to Scale to 4 or 5 Tables
df1 = (spark
    .readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", input['starting_version'])
    .table(f"{input['db_name']}.table1")
) if input['inc_table'] == "1" else (
    spark.table(f"{input['db_name']}.table1")
)

df2 = (spark
    .readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", input['starting_version'])
    .table(f"{input['db_name']}.table2")
) if input['inc_table'] == "2" else (
    spark.table(f"{input['db_name']}.table2")
)

# COMMAND ----------

# DBTITLE 1,[Not Needed in Prod] Display Change Data Feed from Updated Table - Note No Checkpointing
df = df1 if input['inc_table'] == "1" else df2

(df
    .where(f"{input['join_col']} >= {param['display_start']}")
    .writeStream
    .format("memory")
    .trigger(availableNow=True)
    .queryName("inputUDF_console")
    .outputMode("update")
    .start()
    .awaitTermination()
)

display(spark.sql("select * from inputUDF_console"))
# output stream to console https://stackoverflow.com/a/67003316/1890916

# COMMAND ----------

# DBTITLE 1,Define DataFrame for Incremental Join, Excluding State Before Update `update_preimage`
join_update = (df1
    .join(df2, input['join_col'], "inner")
    .where("_change_type != 'update_preimage'")
    .drop("_change_type", "_commit_version", "_commit_timestamp",)
)

# COMMAND ----------

# DBTITLE 1,[Not Needed in Prod] Display Join Update
(join_update
    .where(f"{input['join_col']} >= {param['display_start']}")
    .writeStream
    .format("memory")
    .trigger(availableNow=True)
    .queryName("inputUDF_console")
    .outputMode("update")
    .start()
    .awaitTermination()
)

display(spark.sql("select * from inputUDF_console"))
# output stream to console https://stackoverflow.com/a/67003316/1890916

# COMMAND ----------

# DBTITLE 1,[Not Needed in Prod] Display Joined Dataset Before Merge of Update
display(spark.table(f"{input['db_name']}.joined").where(f"{input['join_col']} >= {param['display_start']}").orderBy(input['join_col']))

# COMMAND ----------

# DBTITLE 1,Streaming Merge Into Joined Table
# Function to perform merge to target table for each microbatch in a stream
def upsertToDelta(microBatchOutputDF, batchId):
    deltaTable = DeltaTable.forPath(spark, f"{s3_prefix}/joined")
    (deltaTable.alias("target")
        .merge(microBatchOutputDF.alias("updates"), f"target.{input['join_col']} = updates.{input['join_col']}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# There is no streaming merge, so call streaming foreachBatch with function defined above
(join_update
    .writeStream
    .option("checkpointLocation", f"{s3_prefix}/checkpoint_join{input['inc_table']}")
    .trigger(availableNow=True)
    .foreachBatch(upsertToDelta)
    .outputMode("update")
    .start()
    .awaitTermination()
)

# COMMAND ----------

# DBTITLE 1,[Not Needed in Prod] Display Joined Dataset After Merge of Update
display(spark.read.load(f"{s3_prefix}/joined").where(f"{input['join_col']} >= {param['display_start']}").orderBy(input['join_col']))
