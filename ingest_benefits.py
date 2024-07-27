# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

import pytz
def parse2(x):
    timezone_mapping = {
        'Atlantic Standard Time': 'America/Halifax',
        'Central Daylight Time': 'US/Central',
        'Central Standard Time': 'US/Central',
        'Cuba Daylight Time': 'America/Havana',
        'Eastern Daylight Time': 'US/Eastern',
        'Eastern Standard Time': 'US/Eastern',
        'Hawaiian Standard Time': 'US/Hawaii',
        'India Standard Time': 'Asia/Kolkata',
        'Mountain Daylight Time': 'US/Mountain',
        'Mountain Standard Time': 'US/Mountain',
        'Pacific Daylight Time': 'US/Pacific',
        'Pacific Standard Time': 'US/Pacific',
        'SA Western Standard Time': 'America/La_Paz',
        'US Eastern Daylight Time': 'US/Eastern',
        'US Mountain Standard Time': 'US/Mountain',
        'Venezuela Standard Time': 'America/Caracas',
        'West Pacific Standard Time': 'Pacific/Port_Moresby',
        'EDT': 'US/Eastern'
    }
    if isinstance(x, float) or x is None:
        return None
    dt_part = x[:22]
    tz_part = x[23:].strip()
    if tz_part == "":
        tz_part = 'Eastern Standard Time'
    try:
        dt = datetime.strptime(dt_part, "%m/%d/%Y %I:%M:%S %p")
        tz = pytz.timezone(timezone_mapping[tz_part])
        localized_dt = tz.localize(dt)
        return localized_dt
    except:
        try: 
            return datetime.strptime(dt_part, "%d-%b-%y")
        except: 
            print(x)
            return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section A

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE mimi_ws_1.partcd.pbp_section_a;

# COMMAND ----------

volumepath = "/Volumes/mimi_ws_1/partcd/src/benefits/"
filepath_lst = sorted([filepath for filepath in Path(volumepath).rglob("pbp_Section_A.txt")], reverse=True)
pdf_lst = []
for filepath in filepath_lst:
    print(filepath)
    mimi_src_file_name = "/".join(str(filepath).split('/')[-2:])
    tokens = str(filepath).split('/')[-2].split('-')
    mimi_src_file_date = parse(f"{tokens[2]}-{int(tokens[-1])*3-2}-01").date()
    pdf = pd.read_csv(filepath, sep="\t", encoding="ISO-8859-1", dtype=str)
    pdf.columns = change_header(pdf.columns)
    pdf["pbp_a_est_memb"] = pd.to_numeric(pdf["pbp_a_est_memb"].apply(lambda x: x if isinstance(x, float) else x.replace(",", "")))
    pdf["pbp_a_contract_period"] = pd.to_numeric(pdf["pbp_a_contract_period"])
    pdf["pbp_a_snp_pct"] = pd.to_numeric(pdf["pbp_a_snp_pct"])
    pdf["pbp_a_bpt_ma_date_time"] = pdf["pbp_a_bpt_ma_date_time"].apply(lambda x: parse2(x))
    pdf["pbp_a_bpt_pd_date_time"] = pdf["pbp_a_bpt_pd_date_time"].apply(lambda x: parse2(x))
    pdf["pbp_a_bpt_msa_date_time"] = pdf["pbp_a_bpt_msa_date_time"].apply(lambda x: parse2(x))
    pdf["pbp_a_bpt_esrd_date_time"] = pdf["pbp_a_bpt_esrd_date_time"].apply(lambda x: parse2(x))
    pdf["pbp_a_upload_date_time"] = pdf["pbp_a_upload_date_time"].apply(lambda x: parse2(x))
    pdf["pbp_a_last_data_entry_date"] = pd.to_datetime(pdf["pbp_a_last_data_entry_date"])
    pdf["mimi_src_file_date"] = mimi_src_file_date
    pdf["mimi_src_file_name"] = mimi_src_file_name
    pdf["mimi_dlt_load_date"] = datetime.today().date()
    pdf_lst.append(pdf)

# COMMAND ----------

pdf_all = pd.concat(pdf_lst)
pdf_all = pdf_all.dropna(axis=1, how='all')
df = spark.createDataFrame(pdf_all)
(df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("mergeSchema", "true")
        .saveAsTable("mimi_ws_1.partcd.pbp_section_a"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part B Drug

# COMMAND ----------

volumepath = "/Volumes/mimi_ws_1/partcd/src/benefits/"
filepath_lst = sorted([filepath for filepath in Path(volumepath).rglob("pbp_b15_partb_rx_drugs.txt")], reverse=True)
pdf_lst = []
for filepath in filepath_lst:
    print(filepath)
    mimi_src_file_name = "/".join(str(filepath).split('/')[-2:])
    tokens = str(filepath).split('/')[-2].split('-')
    mimi_src_file_date = parse(f"{tokens[2]}-{int(tokens[-1])*3-2}-01").date()
    pdf = pd.read_csv(filepath, sep="\t", encoding="ISO-8859-1", dtype=str)
    pdf.columns = change_header(pdf.columns)
    for col in pdf.columns:
        if "_amt" in col or "_pct" in col:
            pdf[col] = pd.to_numeric(pdf[col])
    pdf["mimi_src_file_date"] = mimi_src_file_date
    pdf["mimi_src_file_name"] = mimi_src_file_name
    pdf["mimi_dlt_load_date"] = datetime.today().date()
    pdf_lst.append(pdf)

# COMMAND ----------

pdf_all = pd.concat(pdf_lst)
pdf_all = pdf_all.dropna(axis=1, how='all')
df = spark.createDataFrame(pdf_all)
(df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("mergeSchema", "true")
        .saveAsTable("mimi_ws_1.partcd.pbp_b15_partb_rx_drugs"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Supplemental Benefits

# COMMAND ----------

volumepath = "/Volumes/mimi_ws_1/partcd/src/benefits/"
filepath_lst = sorted([filepath for filepath in Path(volumepath).rglob("pbp_Section_D_opt.txt")], reverse=True)
pdf_lst = []
for filepath in filepath_lst:
    print(filepath)
    mimi_src_file_name = "/".join(str(filepath).split('/')[-2:])
    tokens = str(filepath).split('/')[-2].split('-')
    mimi_src_file_date = parse(f"{tokens[2]}-{int(tokens[-1])*3-2}-01").date()
    pdf = pd.read_csv(filepath, sep="\t", encoding="ISO-8859-1", dtype=str)
    pdf.columns = change_header(pdf.columns)
    for col in pdf.columns:
        if "_amt" in col or "_pct" in col:
            pdf[col] = pd.to_numeric(pdf[col])
    pdf["mimi_src_file_date"] = mimi_src_file_date
    pdf["mimi_src_file_name"] = mimi_src_file_name
    pdf["mimi_dlt_load_date"] = datetime.today().date()
    pdf_lst.append(pdf)

# COMMAND ----------

pdf_all = pd.concat(pdf_lst)
pdf_all = pdf_all.dropna(axis=1, how='all')
df = spark.createDataFrame(pdf_all)
(df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("mergeSchema", "true")
        .saveAsTable("mimi_ws_1.partcd.pbp_section_d_opt"))

# COMMAND ----------

volumepath = "/Volumes/mimi_ws_1/partcd/src/benefits/"
filepath_lst = sorted([filepath for filepath in Path(volumepath).rglob("pbp_b13i_b19b_services_vbid_ssbci.txt")], reverse=True)
pdf_lst = []
for filepath in filepath_lst:
    print(filepath)
    mimi_src_file_name = "/".join(str(filepath).split('/')[-2:])
    tokens = str(filepath).split('/')[-2].split('-')
    mimi_src_file_date = parse(f"{tokens[2]}-{int(tokens[-1])*3-2}-01").date()
    pdf = pd.read_csv(filepath, sep="\t", encoding="ISO-8859-1", dtype=str)
    schema = {}
    for col in pdf.columns:
        if "_amt" in col or "_pct" in col:
            schema[col] = float
        elif "_per" in col and col[-2:] != "_d":
            schema[col] = float
        elif col == "pbp_b13i_ml_max_meals" or col == "pbp_b13i_ml_days":
            schema[col] = float
        else:
            schema[col] = str
    pdf = pd.read_csv(filepath, sep="\t", encoding="ISO-8859-1", dtype=schema)
    pdf.columns = change_header(pdf.columns)
    pdf["mimi_src_file_date"] = mimi_src_file_date
    pdf["mimi_src_file_name"] = mimi_src_file_name
    pdf["mimi_dlt_load_date"] = datetime.today().date()
    pdf_lst.append(pdf)

# COMMAND ----------

pdf_all = pd.concat(pdf_lst)
pdf_all = pdf_all.dropna(axis=1, how='all')
df = spark.createDataFrame(pdf_all)
(df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("mergeSchema", "true")
        .saveAsTable("mimi_ws_1.partcd.pbp_b13i_b19b_services_vbid_ssbci"))

# COMMAND ----------

volumepath = "/Volumes/mimi_ws_1/partcd/src/benefits/"
filepath_lst = sorted([filepath for filepath in Path(volumepath).rglob("pbp_b14c_b19b_preventive_vbid_uf.txt")], reverse=True)
pdf_lst = []
for filepath in filepath_lst:
    print(filepath)
    mimi_src_file_name = "/".join(str(filepath).split('/')[-2:])
    tokens = str(filepath).split('/')[-2].split('-')
    mimi_src_file_date = parse(f"{tokens[2]}-{int(tokens[-1])*3-2}-01").date()
    pdf = pd.read_csv(filepath, sep="\t", encoding="ISO-8859-1", dtype=str)
    schema = {}
    for col in pdf.columns:
        if "_amt" in col or "_pct" in col:
            schema[col] = float
        elif "_per" in col and col[-2:] != "_d":
            schema[col] = float
        else:
            schema[col] = str
    pdf = pd.read_csv(filepath, sep="\t", encoding="ISO-8859-1", dtype=schema)
    pdf.columns = change_header(pdf.columns)
    pdf["mimi_src_file_date"] = mimi_src_file_date
    pdf["mimi_src_file_name"] = mimi_src_file_name
    pdf["mimi_dlt_load_date"] = datetime.today().date()
    pdf_lst.append(pdf)

# COMMAND ----------

pdf_all = pd.concat(pdf_lst)
pdf_all = pdf_all.dropna(axis=1, how='all')
df = spark.createDataFrame(pdf_all)
(df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("mergeSchema", "true")
        .saveAsTable("mimi_ws_1.partcd.pbp_b14c_b19b_preventive_vbid_uf"))

# COMMAND ----------

pdf_all["pbp_b14c_bendesc_lim_cs"].unique()

# COMMAND ----------


