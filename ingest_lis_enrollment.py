# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

volumepath = "/Volumes/mimi_ws_1/partcd/src/lis_enrollment/"

# COMMAND ----------

files_existing = {x[0] for x in spark.read.table('mimi_ws_1.partcd.lis_enrollment_by_plan').select('mimi_src_file_name').distinct().collect()}

# COMMAND ----------

files_existing

# COMMAND ----------

for filepath in Path(volumepath).rglob("*by [Cc]ontract*.xlsx"):

    if filepath.name in files_existing:
        continue

    #pd.read_excel(filepath)
    year = (str(filepath).split('/src/lis_enrollment/')[1][:4])
    if year < '2015':
        continue

    pdf_lst = []
    for sheet_num in range(2):
        pdf = pd.read_excel(filepath, sheet_name=sheet_num, dtype=str)
        pdf.columns = ['contract_id', 'contract_name', 'pbp_id', 'pbp_name', 'enrolled', 'lis_enrolled']
        pdf['enrolled'] = pd.to_numeric(pdf['enrolled'].str.replace(',',''), 
                                         errors='coerce').astype('Int64')
        pdf['lis_enrolled'] = pd.to_numeric(pdf['lis_enrolled'].str.replace(',',''), 
                                         errors='coerce').astype('Int64')
        pdf_lst.append(pdf)
    
    pdf_full = pd.concat(pdf_lst)
    pdf_full['mimi_src_file_date'] = parse(f"{year}-12-31").date()
    pdf_full['mimi_src_file_name'] = filepath.name
    pdf_full['mimi_dlt_load_date'] = datetime.today().date()
    (
        spark.createDataFrame(pdf_full)
            .write
            .mode('overwrite')
            .option('replaceWhere', f"mimi_src_file_name = '{filepath.name}'")
            .saveAsTable('mimi_ws_1.partcd.lis_enrollment_by_plan')
    )

