# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

volumepath = "/Volumes/mimi_ws_1/partcd/src/plancrosswalk/"

# COMMAND ----------

file_lst = sorted([filepath for filepath in Path(volumepath).rglob("PlanCrosswalk*.xlsx")], 
                        key=lambda x: x.parent.stem[-4:], 
                        reverse=True)

# COMMAND ----------

files_existing = {x[0] for x in spark.read.table('mimi_ws_1.partcd.plan_crosswalk').select('mimi_src_file_name').distinct().collect()}

# COMMAND ----------

for filepath in file_lst:
    if filepath.name in files_existing:
        continue
    pdf = pd.read_excel(filepath, dtype=str)
    performance_date_start = parse(filepath.parent.stem[-4:] + '-01-01').date()
    performance_date_end = parse(filepath.parent.stem[-4:] + '-12-31').date()
    mimi_src_file_date = datetime.strptime(filepath.stem[-8:], '%m%d%Y').date()
    mimi_src_file_name = filepath.name
    mimi_dlt_load_date = datetime.today().date()
    pdf.columns = change_header(pdf)
    pdf.rename(columns={'description': 'status'}, inplace=True)
    pdf['current_plan_id'] = pdf['current_plan_id'].str.zfill(3)
    pdf['previous_plan_id'] = pdf['previous_plan_id'].str.zfill(3)
    pdf['performance_date_start'] = performance_date_start
    pdf['performance_date_end'] = performance_date_end
    pdf['mimi_src_file_date'] = mimi_src_file_date
    pdf['mimi_src_file_name'] = mimi_src_file_name
    pdf['mimi_dlt_load_date'] = mimi_dlt_load_date
    (
        spark.createDataFrame(pdf)
            .write
            .mode('overwrite')
            .option('replaceWhere', f"mimi_src_file_name = '{filepath.name}'")
            .option('mergeSchema', 'true')
            .saveAsTable('mimi_ws_1.partcd.plan_crosswalk')
    )

# COMMAND ----------


