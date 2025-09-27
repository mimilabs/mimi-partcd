# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

volumepath = "/Volumes/mimi_ws_1/partcd/src/directory/"

# COMMAND ----------

files_exiting = {x[0] for x in spark.read.table('mimi_ws_1.partcd.mapd_plan_directory') \
                                .select('mimi_src_file_name') \
                                .distinct().collect()}

# COMMAND ----------

colmap = {'legal_entity_st2': 'legal_entity_street2'}
for filepath in Path(volumepath).rglob("*.csv"):
    if filepath.name in files_exiting:
        continue
    mimi_src_file_date = parse(filepath.stem[-7:].replace('_', '-') 
                               + '-01').date()
    pdf = pd.read_csv(filepath, dtype=str)
    pdf.columns = [colmap.get(x, x) for x in change_header(pdf)]
    pdf['enrollment'] = pd.to_numeric(pdf['enrollment'].str.replace(',', ''), errors='coerce').astype('Int64')
    pdf['contract_effective_date'] = pd.to_datetime(pdf['contract_effective_date'], format='%m/%d/%Y').dt.date
    pdf['directory_contact_last_update'] = pd.to_datetime(pdf['directory_contact_last_update'], format='%m/%d/%Y').dt.date
    pdf['mimi_src_file_date'] = mimi_src_file_date
    pdf['mimi_src_file_name'] = filepath.name
    pdf['mimi_dlt_load_date'] = datetime.today().date()
    (
        spark.createDataFrame(pdf)
            .write
            .mode('overwrite')
            .option('replaceWhere', f"mimi_src_file_name = '{filepath.name}'")
            .saveAsTable('mimi_ws_1.partcd.mapd_plan_directory')
    )

# COMMAND ----------


