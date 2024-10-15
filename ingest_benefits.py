# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

# MAGIC %run /Workspace/utils/basic

# COMMAND ----------

volumepath = "/Volumes/mimi_ws_1/partcd/src/benefits/"

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE IF EXISTS mimi_ws_1.partcd.pbp_section_a;
# MAGIC --DROP TABLE IF EXISTS mimi_ws_1.partcd.pbp_b13i_b19b_services_vbid_ssbci;
# MAGIC --DROP TABLE IF EXISTS mimi_ws_1.partcd.pbp_b14c_b19b_preventive_vbid_uf;
# MAGIC --DROP TABLE IF EXISTS mimi_ws_1.partcd.pbp_b15_partb_rx_drugs;
# MAGIC --DROP TABLE IF EXISTS mimi_ws_1.partcd.pbp_section_d_opt;

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

def generic_ingestion(volumepath, filename, pdf_metadata):
    filepath_lst = sorted([filepath for filepath in Path(volumepath).rglob(f"{filename}.txt")], reverse=True)
    pdf_lst = []
    for filepath in filepath_lst:
        print(filepath)
        mimi_src_file_name = filepath.parent.stem + '/' + filepath.name
        tokens = str(filepath.parent.stem).split('-')
        mimi_src_file_date = parse(f"{tokens[2]}-{int(tokens[-1])*3-2}-01").date()
        numeric_columns = (pdf_metadata.loc[(pdf_metadata['file']==filename),:]
                    .groupby('name')['type']
                    .apply(list)
                    .reset_index()
                    .loc[lambda x: x['type'].apply(
                            lambda y: all(x_i == 'NUM' for x_i in y))]
                    )['name'].to_list()
        numeric_columns = set([x for x in numeric_columns 
                   if x!='version' and not x.endswith('_id')])
        pdf = pd.read_csv(filepath, sep="\t", encoding="ISO-8859-1", dtype=str,
                          quoting=csv.QUOTE_NONE)
        schema = {}
        for col in pdf.columns:
            if col in numeric_columns:
                schema[col] = float
            else:
                schema[col] = str
        pdf = pd.read_csv(filepath, sep="\t", encoding="ISO-8859-1", dtype=schema,
                          quoting=csv.QUOTE_NONE, 
                          thousands=',')
        pdf["mimi_src_file_date"] = mimi_src_file_date
        pdf["mimi_src_file_name"] = mimi_src_file_name
        pdf["mimi_dlt_load_date"] = datetime.today().date()
        pdf_lst.append(pdf)

    pdf_all = pd.concat(pdf_lst)
    pdf_all = pdf_all.dropna(axis=1, how='all')
    df = spark.createDataFrame(pdf_all)
    columns = ([x for x in df.columns if not x.startswith('mimi_')] + 
            ['mimi_src_file_date', 'mimi_src_file_name', 'mimi_dlt_load_date'])
    (df.select(*columns).write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("mergeSchema", "true")
            .saveAsTable(f"mimi_ws_1.partcd.{filename}"))

# COMMAND ----------

def add_column_desc(filename, pdf_metadata):
    pdf_desc = (pdf_metadata.loc[(pdf_metadata['file']==filename),:]
                        .drop_duplicates(subset=['name']))
    df = spark.read.table(f"mimi_ws_1.partcd.{filename.lower()}")
    for _, row in pdf_desc.iterrows():
        if row['name'] not in df.columns:
            continue
        desc = ''
        if row['title']:
            desc = row['title'].replace("'", "\\'")
        if row['field_title']:
            desc += ('; ' + row['field_title'].replace("'", "\\'"))
        stmt = (f"ALTER TABLE mimi_ws_1.partcd.{filename.lower()} ALTER COLUMN " + 
                f"{row['name']} COMMENT '{desc}';")
        spark.sql(stmt)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metadata/Dictionary
# MAGIC

# COMMAND ----------


filepath_lst = sorted([filepath for filepath in Path(volumepath).rglob("*dictionary.xlsx")], reverse=True)
for filepath in filepath_lst:
    
    tokens = str(filepath.parent.stem).split('-')
    mimi_src_file_name = filepath.parent.stem + '/' + filepath.name
    mimi_src_file_date = parse(f"{tokens[2]}-{int(tokens[-1])*3-2}-01").date()
    pdf = pd.read_excel(filepath, dtype=str)
    pdf.columns = change_header(pdf.columns)
    pdf['code_and_values'] = pdf['codes'] + ': ' + pdf["code_values"]
    pdf_cv = (pdf.dropna(subset=['codes', 'code_values'])
              .groupby(by=['file', 'name'])['code_and_values']
              .apply(list).reset_index())
    pdf_cv['code_and_values'] = pdf_cv['code_and_values'].apply(lambda x: '; '.join(x))
    pdf = (pdf.dropna(subset=['type', 'length'])
            .drop(columns=['codes', 'code_values', 'code_and_values'])
            .merge(pdf_cv, on=['file', 'name'], how='left'))
    pdf['mimi_src_file_date'] = mimi_src_file_date
    pdf['mimi_src_file_name'] = mimi_src_file_name
    pdf['mimi_dlt_load_date'] = datetime.today().date()
    df = spark.createDataFrame(pdf)
    (df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"mimi_src_file_name = '{mimi_src_file_name}'")
        .saveAsTable("mimi_ws_1.partcd.pbp_metadata"))
    print(mimi_src_file_name)

# COMMAND ----------

pdf_metadata = spark.read.table('mimi_ws_1.partcd.pbp_metadata').toPandas()

# COMMAND ----------

volumepath = "/Volumes/mimi_ws_1/partcd/src/benefits/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section A

# COMMAND ----------

filename = 'pbp_Section_A'
filepath_lst = sorted([filepath for filepath in Path(volumepath).rglob(f"{filename}.txt")], reverse=True)
pdf_lst = []
for filepath in filepath_lst:
    print(filepath)
    mimi_src_file_name = "/".join(str(filepath).split('/')[-2:])
    tokens = str(filepath.parent.stem).split('-')
    mimi_src_file_date = parse(f"{tokens[2]}-{int(tokens[-1])*3-2}-01").date()
    pdf = pd.read_csv(filepath, sep="\t", encoding="ISO-8859-1", dtype=str)
    pdf.columns = change_header(pdf.columns)
    numeric_columns = set(pdf_metadata.loc[((pdf_metadata['file']==filename) & 
                  (pdf_metadata['type']=='NUM') &
                  (pdf_metadata['name']!='version') & 
                  ~(pdf_metadata['name'].str.endswith('_id'))),'name'].to_list())
    for col in numeric_columns:
        pdf[col] = pd.to_numeric(pdf[col].apply(lambda x: x if isinstance(x, float) else x.replace(",", "")))
    if "pbp_a_contract_period" in pdf.columns:
        pdf["pbp_a_contract_period"] = pd.to_numeric(pdf["pbp_a_contract_period"])
    pdf["pbp_a_snp_pct"] = pd.to_numeric(pdf["pbp_a_snp_pct"])
    pdf["pbp_a_bpt_ma_date_time"] = pdf["pbp_a_bpt_ma_date_time"].apply(lambda x: parse2(x))
    pdf["pbp_a_bpt_pd_date_time"] = pdf["pbp_a_bpt_pd_date_time"].apply(lambda x: parse2(x))
    pdf["pbp_a_bpt_msa_date_time"] = pdf["pbp_a_bpt_msa_date_time"].apply(lambda x: parse2(x))
    pdf["pbp_a_bpt_esrd_date_time"] = pdf["pbp_a_bpt_esrd_date_time"].apply(lambda x: parse2(x))
    pdf["pbp_a_upload_date_time"] = pdf["pbp_a_upload_date_time"].apply(lambda x: parse2(x))
    if "pbp_a_last_data_entry_date" in pdf.columns:
        pdf["pbp_a_last_data_entry_date"] = pd.to_datetime(pdf["pbp_a_last_data_entry_date"])
    pdf["mimi_src_file_date"] = mimi_src_file_date
    pdf["mimi_src_file_name"] = mimi_src_file_name
    pdf["mimi_dlt_load_date"] = datetime.today().date()
    pdf_lst.append(pdf)

pdf_all = pd.concat(pdf_lst)
pdf_all = pdf_all.dropna(axis=1, how='all')
df = spark.createDataFrame(pdf_all)
columns = ([x for x in df.columns if not x.startswith('mimi_')] + 
            ['mimi_src_file_date', 'mimi_src_file_name', 'mimi_dlt_load_date'])
(df.select(*columns).write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("mergeSchema", "true")
        .saveAsTable(f"mimi_ws_1.partcd.{filename.lower()}"))
add_column_desc(filename, pdf_metadata)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part B Drug

# COMMAND ----------

filename = 'pbp_b15_partb_rx_drugs'
generic_ingestion(volumepath, filename, pdf_metadata)
add_column_desc(filename, pdf_metadata)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section D

# COMMAND ----------

filename = 'pbp_Section_D'
generic_ingestion(volumepath, filename, pdf_metadata)
add_column_desc(filename, pdf_metadata)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Supplemental Benefits

# COMMAND ----------

filename = 'pbp_Section_D_opt'
generic_ingestion(volumepath, filename, pdf_metadata)
add_column_desc(filename, pdf_metadata)

# COMMAND ----------

# MAGIC %md
# MAGIC ## VBID

# COMMAND ----------

filename = 'pbp_b13i_b19b_services_vbid_ssbci'
generic_ingestion(volumepath, filename, pdf_metadata)
add_column_desc(filename, pdf_metadata)

# COMMAND ----------

filename = 'pbp_b14c_b19b_preventive_vbid_uf'
generic_ingestion(volumepath, filename, pdf_metadata)
add_column_desc(filename, pdf_metadata)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dental

# COMMAND ----------

filename = 'pbp_b16_dental'
generic_ingestion(volumepath, filename, pdf_metadata)
add_column_desc(filename, pdf_metadata)

filename = 'pbp_b16_b19b_dental_vbid_uf'
generic_ingestion(volumepath, filename, pdf_metadata)
add_column_desc(filename, pdf_metadata)

# COMMAND ----------

# MAGIC %md
# MAGIC ## MRX

# COMMAND ----------

filename = 'pbp_mrx'
generic_ingestion(volumepath, filename, pdf_metadata)
add_column_desc(filename, pdf_metadata)

filename = 'pbp_mrx_vbid'
generic_ingestion(volumepath, filename, pdf_metadata)
add_column_desc(filename, pdf_metadata)

filename = 'pbp_mrx_gapCoverage'
generic_ingestion(volumepath, filename, pdf_metadata)
add_column_desc(filename, pdf_metadata)

filename = 'pbp_mrx_p'
generic_ingestion(volumepath, filename, pdf_metadata)
add_column_desc(filename, pdf_metadata)

filename = 'pbp_mrx_p_vbid'
generic_ingestion(volumepath, filename, pdf_metadata)
add_column_desc(filename, pdf_metadata)

filename = 'pbp_mrx_tier'
generic_ingestion(volumepath, filename, pdf_metadata)
add_column_desc(filename, pdf_metadata)

filename = 'pbp_mrx_tier_vbid'
generic_ingestion(volumepath, filename, pdf_metadata)
add_column_desc(filename, pdf_metadata)

# COMMAND ----------

# MAGIC %md
# MAGIC ## MISC

# COMMAND ----------

filename = 'pbp_b10_amb_trans'
generic_ingestion(volumepath, filename, pdf_metadata)
add_column_desc(filename, pdf_metadata)

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_metadata IS '# [Medicare Advantage Benefits Information Metadata](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data) -  Contains the list of tables, columns, and their descriptions.; multiquarter
# MAGIC ';
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_section_a IS '# [Medicare Advantage Benefits Information Section A](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data) -  Contains general plan information.; multiquarter
# MAGIC ';
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_b13i_b19b_services_vbid_ssbci IS '# [Medicare Advantage Benefits Information Section B13I-B19B VBID](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data) -  Non-Primarily Health Related Benefits for the Chronically III, Food and Produce, Meals, Pest Control, Transportation for Non-Medical Needs, Indoor Air Quality Equipment and Services and Other data.; multiquarter
# MAGIC ';
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_b14c_b19b_preventive_vbid_uf IS '# [Medicare Advantage Benefits Information Section B14C-B19B VBID](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data) -  Contains Supplemental Benefits Preventive Services VBID and UF data for Health Education/Wellness, Fitness Benefit, Counseling Services.; multiquarter
# MAGIC ';
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_b15_partb_rx_drugs IS '# [Medicare Advantage Benefits Information Section B15 PartB Rx](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data) -  Medicare Part B prescription drugs.; multiquarter
# MAGIC ';
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_section_d_opt IS '# [Medicare Advantage Benefits Information Section D Optional](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data) -  Contains plan-level benefit data descriptions of optional supplemental offerings including the optional benefit premium amounts, multiquarter
# MAGIC ';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_b16_dental IS '# [Medicare Advantage Benefits Information Section B16 Dental](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data) - Preventive and Comprehensive Dental data, multiquarter';
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_b16_b19b_dental_vbid_uf IS '# [Medicare Advantage Benefits Information Section B16-B19B Dental VBID UF](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data) - Preventive and Comprehensive Dental VBID and UF data, multiquarter';
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_mrx IS '# [Medicare Advantage Benefits Information Section MRX](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data) - Contains Medicare Part D prescription drug benefits data, multiquarter';
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_mrx_vbid IS '# [Medicare Advantage Benefits Information Section MRX VBID](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data) - Contains Medicare Part D prescription drug benefits VBID data, multiquarter';
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_mrx_gapCoverage IS '# [Medicare Advantage Benefits Information Section MRX Gap Coverage](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data) - Contains information on coverage of either tier(s) or specific drugs through the entire gap (ICL to catastrophic), multiquarter';
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_mrx_p IS '# [Medicare Advantage Benefits Information Section B14C-B19B MRX P](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data) - Contains Medicare Part D prescription drug benefits (post OOP threshold) data, multiquarter';
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_mrx_p_vbid IS '# [Medicare Advantage Benefits Information Section MRX P VBID](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data) - Contains Medicare Part D prescription drug benefits (post OOP threshold) VBID data, multiquarter';
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_mrx_tier IS '# [Medicare Advantage Benefits Information Section MRX TIER](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data) - Contains Medicare Part D prescription drug benefits (tiering) data, multiquarter';
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_mrx_tier_vbid IS '# [Medicare Advantage Benefits Information Section MRX TIER VBID VBID](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data) - Contains Medicare Part D prescription drug benefits (tiering) data, multiquarter';

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_mrx_tier_vbid IS '# [Medicare Advantage Benefits Information Section B10 Ambulance/Transportation data](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data) - Ambulance/Transportation data benefits (tiering) data, multiquarter';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plan Areas

# COMMAND ----------

filename = "PlanArea"
filepath_lst = sorted([filepath for filepath in Path(volumepath).rglob(f"{filename}.txt")], 
                      reverse=True)
pdf_lst = []
for filepath in filepath_lst:
    print(filepath)
    mimi_src_file_name = filepath.parent.stem + '/' + filepath.name
    tokens = str(filepath.parent.stem).split('-')
    mimi_src_file_date = parse(f"{tokens[2]}-{int(tokens[-1])*3-2}-01").date()

    pdf = pd.read_csv(filepath, sep="\t", encoding="ISO-8859-1", dtype=str,
                        quoting=csv.QUOTE_NONE)
    pdf['contract_year'] = pd.to_numeric(pdf['contract_year']).astype('int')
    pdf = pdf.dropna(axis=1, how='all')
    pdf['mimi_src_file_date'] = mimi_src_file_date
    pdf['mimi_src_file_name'] = mimi_src_file_name
    pdf['mimi_dlt_load_date'] = datetime.today().date()
    df = spark.createDataFrame(pdf)
    (df.write.mode("overwrite")
        .option('replaceWhere', f"mimi_src_file_name = '{mimi_src_file_name}'")
        .saveAsTable("mimi_ws_1.partcd.pbp_plan_area"))

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_plan_area IS '# [Medicare Advantage Benefits Information Section PlanArea Files](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data) - Contains Service Area data by plan (includes EGHP service areas), multiquarter';

# COMMAND ----------

filename = "PlanRegionArea"
filepath_lst = sorted([filepath for filepath in Path(volumepath).rglob(f"{filename}.txt")], 
                      reverse=True)
pdf_lst = []
for filepath in filepath_lst:
    print(filepath)
    mimi_src_file_name = filepath.parent.stem + '/' + filepath.name
    tokens = str(filepath.parent.stem).split('-')
    mimi_src_file_date = parse(f"{tokens[2]}-{int(tokens[-1])*3-2}-01").date()

    pdf = pd.read_csv(filepath, sep="\t", encoding="ISO-8859-1", dtype=str,
                        quoting=csv.QUOTE_NONE)
    pdf['contract_year'] = pd.to_numeric(pdf['contract_year']).astype('int')
    pdf = pdf.dropna(axis=1, how='all')
    pdf['mimi_src_file_date'] = mimi_src_file_date
    pdf['mimi_src_file_name'] = mimi_src_file_name
    pdf['mimi_dlt_load_date'] = datetime.today().date()
    df = spark.createDataFrame(pdf)
    (df.write.mode("overwrite")
        .option('replaceWhere', f"mimi_src_file_name = '{mimi_src_file_name}'")
        .saveAsTable("mimi_ws_1.partcd.pbp_plan_region_area"))

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_plan_region_area IS '# [Medicare Advantage Benefits Information Section PlanRegionArea Files](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data) - Contains Service Region Area data by plan (Regional MA plans and PDPs), multiquarter';

# COMMAND ----------


