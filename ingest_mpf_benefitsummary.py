# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

# MAGIC %pip install pyxlsb

# COMMAND ----------

volumepath = "/Volumes/mimi_ws_1/partcd/src/mpf_benefit_summary/"

# COMMAND ----------

def extract_cy_year(string):
    # Pattern matches CY followed by exactly 4 digits
    pattern = r'CY\d{4}'

    match = re.search(pattern, string)
    if match:
        return match.group()[2:]
    return None

# COMMAND ----------

files_exist = {}
if spark.catalog.tableExists('mimi_ws_1.partcd.mpf_benefit_summary'):
    files_exist = {row.mimi_src_file_name for row 
                   in spark.table('mimi_ws_1.partcd.mpf_benefit_summary').select('mimi_src_file_name').distinct().collect()}

# COMMAND ----------

colmap = {'service_name': 'service_desc'}

# COMMAND ----------

files_exist

# COMMAND ----------

for filepath in Path(volumepath).rglob("*MPF_[Bb]enefit*.xls*"):
    if filepath.stem.startswith('.'):
        continue
    if filepath.name in files_exist:
        continue
    
    mimi_src_file_date = parse(extract_cy_year(filepath.stem) + '-01-01').date()

    pdf = pd.read_excel(filepath, dtype=str)
    pdf.columns = [colmap.get(x, x) for x in change_header(pdf.columns)]
    pdf['mimi_src_file_date'] = mimi_src_file_date
    pdf['mimi_src_file_name'] = filepath.name
    pdf['mimi_dlt_load_date'] = datetime.today().date()
    (
        spark.createDataFrame(pdf)
            .write
            .mode('overwrite')
            .option('replaceWhere', f"mimi_src_file_name = '{filepath.name}'")
            .saveAsTable('mimi_ws_1.partcd.mpf_benefit_summary')
    )

# COMMAND ----------


