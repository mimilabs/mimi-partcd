# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

volumepath = "/Volumes/mimi_ws_1/partcd/src/snp"

# COMMAND ----------

file_lst = sorted([filepath for filepath in Path(volumepath).rglob("*.xlsx")
    if not filepath.stem.startswith('Raw')],
    key = lambda x: x.parent.stem,
    reverse=True)

# COMMAND ----------

files_existing = {x[0] for x in spark.read.table('mimi_ws_1.partcd.snpdata').select('mimi_src_file_name').distinct().collect()}

# COMMAND ----------

for filepath in file_lst:

    mimi_src_file_date = parse(filepath.parent.stem[-7:].replace('_','-') 
                               + '-01').date()
    mimi_src_file_name = filepath.parent.stem + '/' + filepath.name

    if mimi_src_file_name in files_existing:
        continue

    skiprows = 0
    for i in range(30):
        pdf = pd.read_excel(filepath, dtype=str, skiprows=i, sheet_name=0)
        if len(pdf.columns) > 0 and pdf.columns[0] == "Contract Number":
            skiprows = i
            break

    if filepath.parent.stem[-7:] == '2023_10':
        pdf = pd.read_excel(filepath, dtype=str, 
                            skiprows=(skiprows+1), sheet_name=0)
        pdf.rename(columns={'Enrollment': 'Plan Enrollment'}, 
                   inplace=True)
    
    pdf.columns = change_header(pdf.columns)
    pdf['plan_enrollment'] = pd.to_numeric(pdf['plan_enrollment'], 
                                           errors='coerce')
    col_drops = [col for col in pdf.columns if col.startswith('unnamed')]
    if len(col_drops) > 0:
        pdf.drop(columns=col_drops, inplace=True)

    if 'plan_geographic_name' in pdf.columns:
        pdf.rename(columns={'plan_geographic_name': 'geographic_name'},   
                   inplace=True)

    pdf['mimi_src_file_date'] = mimi_src_file_date
    pdf['mimi_src_file_name'] = mimi_src_file_name
    pdf['mimi_dlt_load_date'] = datetime.today().date()
    (
        spark.createDataFrame(pdf)
            .write
            .mode('overwrite')
            .option('mergeSchema', 'true')
            .option('replaceWhere', f"mimi_src_file_name = '{mimi_src_file_name}'")
            .saveAsTable('mimi_ws_1.partcd.snpdata')
    )

# COMMAND ----------


