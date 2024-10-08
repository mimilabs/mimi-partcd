# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the CPSC MA enrollments
# MAGIC
# MAGIC - Enrollment Files
# MAGIC - Contract Files

# COMMAND ----------

# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

from pyspark.sql.functions import col, lit, to_date, regexp_replace, substring
path = "/Volumes/mimi_ws_1/partcd/src/" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "partcd" # delta table destination schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrollment Data

# COMMAND ----------

tablename = "cpsc_enrollment" # destination table

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE mimi_ws_1.partcd.cpsc_enrollment;

# COMMAND ----------

# We want to skip those files that are already in the delta tables.
# We look up the table, and see if the files are already there or not.
files_exist = {}
if spark.catalog.tableExists(f"{catalog}.{schema}.{tablename}"):
    files_exist = set([row["mimi_src_file_date"] 
                   for row in 
                   (spark.read.table(f"{catalog}.{schema}.{tablename}")
                            .select("mimi_src_file_date")
                            .distinct()
                            .collect())])

# COMMAND ----------

files = []
for filepath in Path(path+"/cpsc").rglob("CPSC_Enrollment_*.csv"):
    year = filepath.stem[-7:-3]
    month = filepath.stem[-2:]
    dt = parse(f"{year}-{month}-01").date()
    if dt not in files_exist:
        files.append((dt, filepath))
files = sorted(files, key=lambda x: x[0], reverse=True)

# COMMAND ----------

int_columns = {"enrollment"}
legacy_columns = {}
for item in files:
    # each file is relatively big
    # so we load the data using spark one by one just in case
    # the size hits a single machine memory
    df = (spark.read.format("csv")
            .option("header", "true")
            .load(str(item[1])))
    header = []
    columns = df.columns
    for col_old, col_new_ in zip(columns, change_header(columns)):
        
        col_new = legacy_columns.get(col_new_, col_new_)
        header.append(col_new)
        
        if col_new in int_columns:
            col_old_tmp = (col_old+"_")
            df = (df.withColumnRenamed(col_old, col_old_tmp)
                  .filter(col(col_old_tmp) != "*")
                  .withColumn(col_new, col(col_old_tmp).cast('int')))
        else:
            df = df.withColumn(col_new, col(col_old))
            
    df = (df.select(*header)
        .withColumn("mimi_src_file_date", lit(item[0]))
        .withColumn("mimi_src_file_name", lit(item[1].name))
        .withColumn("mimi_dlt_load_date", lit(datetime.today().date()))
        .filter(col("enrollment").isNotNull()))
    
    # Some of these are manually corrected above. However, just in case
    # we make the mergeSchema option "true"
    mimi_src_file_date_str = item[0].strftime("%Y-%m-%d")
    (df.write
        .format('delta')
        .mode("overwrite")
        .option("mergeSchema", "true")
        .option("replaceWhere", f"mimi_src_file_date = '{mimi_src_file_date_str}'")
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contract Information

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE mimi_ws_1.partcd.cpsc_contract;

# COMMAND ----------

tablename = "cpsc_contract" # destination table
# We want to skip those files that are already in the delta tables.
# We look up the table, and see if the files are already there or not.
files_exist = {}
if spark.catalog.tableExists(f"{catalog}.{schema}.{tablename}"):
    files_exist = set([row["mimi_src_file_date"] 
                   for row in 
                   (spark.read.table(f"{catalog}.{schema}.{tablename}")
                            .select("mimi_src_file_date")
                            .distinct()
                            .collect())])
files = []
for filepath in Path(f"{path}/cpsc").rglob("CPSC_Contract_*.csv"):
    year = filepath.stem[-7:-3]
    month = filepath.stem[-2:]
    dt = parse(f"{year}-{month}-01").date()
    if dt not in files_exist:
        files.append((dt, filepath))
files = sorted(files, key=lambda x: x[0], reverse=True)

# COMMAND ----------

date_columns = {"contract_effective_date"}
legacy_columns = {}
for item in files:
    # each file is relatively big
    # so we load the data using spark one by one just in case
    # the size hits a single machine memory
    df = (spark.read.format("csv")
            .option("header", "true")
            .load(str(item[1])))
    header = []
    for col_old, col_new_ in zip(df.columns, change_header(df.columns)):
        
        col_new = legacy_columns.get(col_new_, col_new_)
        header.append(col_new)
        
        if col_new in date_columns:
            df = df.withColumn(col_new, to_date(substring(col(col_old),1,10), "MM/dd/yyyy"))
        else:
            df = df.withColumn(col_new, col(col_old))
            
    df = (df.select(*header)
           .withColumn("mimi_src_file_date", lit(item[0]))
            .withColumn("mimi_src_file_name", lit(item[1].name))
            .withColumn("mimi_dlt_load_date", lit(datetime.today().date())))
    
    # Some of these are manually corrected above. However, just in case
    # we make the mergeSchema option "true"
    mimi_src_file_date_str = item[0].strftime("%Y-%m-%d")
    (df.write
        .format('delta')
        .mode("overwrite")
        .option("mergeSchema", "true")
        .option("replaceWhere", f"mimi_src_file_date = '{mimi_src_file_date_str}'")
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine them

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE mimi_ws_1.partcd.cpsc_combined;

# COMMAND ----------

from pyspark.sql.functions import (col, lit, max as _max)
catalog = "mimi_ws_1" # delta table destination catalog
schema = "partcd" # delta table destination schema
df_enroll = (spark.read.table(f"{catalog}.{schema}.cpsc_enrollment")
                .withColumnRenamed("contract_number", "contract_id")
                .filter(col("enrollment").isNotNull()))
                
df_contract = (spark.read.table(f"{catalog}.{schema}.cpsc_contract")
                .filter(col("contract_id").isNotNull())
                .filter(col("plan_id").isNotNull())
                .drop("mimi_src_file_name")
                .drop("mimi_dlt_load_date")
                .dropDuplicates(["contract_id", "plan_id", "mimi_src_file_date"]))

df_combined = df_enroll.join(df_contract, 
                             on=["contract_id", "plan_id", "mimi_src_file_date"], 
                             how="left")
(df_combined.write
        .format('delta')
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.cpsc_combined"))

# COMMAND ----------


