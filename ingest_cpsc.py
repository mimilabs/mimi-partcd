# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the CPSC MA enrollments
# MAGIC
# MAGIC - Enrollment Files
# MAGIC - Contract Files

# COMMAND ----------

from pathlib import Path
from pyspark.sql.functions import col, lit, to_date, regexp_replace
from datetime import datetime
from dateutil.parser import parse
import re

path = "/Volumes/mimi_ws_1/partcd/src/" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "partcd" # delta table destination schema


# COMMAND ----------

def change_header(header_org):
    return [re.sub(r'\W+', '', column.lower().replace(' ','_'))
            for column in header_org]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrollment Data

# COMMAND ----------

tablename = "cpsc_enrollment" # destination table

# COMMAND ----------

# We want to skip those files that are already in the delta tables.
# We look up the table, and see if the files are already there or not.
files_exist = {}
writemode = "overwrite"
if spark.catalog.tableExists(f"{catalog}.{schema}.{tablename}"):
    files_exist = set([row["_input_file_date"] 
                   for row in 
                   (spark.read.table(f"{catalog}.{schema}.{tablename}")
                            .select("_input_file_date")
                            .distinct()
                            .collect())])
    writemode = "append"

# COMMAND ----------

files = []
for folderpath in Path(f"{path}/cpsc").glob("*"):
    for filepath in Path(f"{folderpath}").glob("CPSC_Enrollment_*"):
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
    for col_old, col_new_ in zip(df.columns, change_header(df.columns)):
        
        col_new = legacy_columns.get(col_new_, col_new_)
        header.append(col_new)
        
        if col_new in int_columns:
            df = df.withColumn(col_new, regexp_replace(col(col_old), "[\*\$,%]", "").cast("int"))
        else:
            df = df.withColumn(col_new, col(col_old))
            
    df = (df.select(*header)
          .withColumn("_input_file_date", lit(item[0]))
          .filter(col("enrollment").isNotNull()))
    
    # Some of these are manually corrected above. However, just in case
    # we make the mergeSchema option "true"
    (df.write
        .format('delta')
        .mode(writemode)
        .option("mergeSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))
    
    writemode="append"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contract Information

# COMMAND ----------

tablename = "cpsc_contract" # destination table
# We want to skip those files that are already in the delta tables.
# We look up the table, and see if the files are already there or not.
files_exist = {}
writemode = "overwrite"
if spark.catalog.tableExists(f"{catalog}.{schema}.{tablename}"):
    files_exist = set([row["_input_file_date"] 
                   for row in 
                   (spark.read.table(f"{catalog}.{schema}.{tablename}")
                            .select("_input_file_date")
                            .distinct()
                            .collect())])
    writemode = "append"
files = []
for folderpath in Path(f"{path}/cpsc").glob("*"):
    for filepath in Path(f"{folderpath}").glob("CPSC_Contract_*"):
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
            df = df.withColumn(col_new, to_date(col(col_old), "MM/dd/yyyy"))
        else:
            df = df.withColumn(col_new, col(col_old))
            
    df = (df.select(*header)
          .withColumn("_input_file_date", lit(item[0])))
    
    # Some of these are manually corrected above. However, just in case
    # we make the mergeSchema option "true"
    (df.write
        .format('delta')
        .mode(writemode)
        .option("mergeSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))
    
    writemode="append"

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE mimi_ws_1.partcd.cpsc_enrollment;
# MAGIC OPTIMIZE mimi_ws_1.partcd.cpsc_contract;

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
                .dropDuplicates(["contract_id", "plan_id", "_input_file_date"]))

df_combined = df_enroll.join(df_contract, 
                             on=["contract_id", "plan_id", "_input_file_date"], 
                             how="left")
(df_combined.write
        .format('delta')
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.cpsc_combined"))

# COMMAND ----------


