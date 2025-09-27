# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

# MAGIC %run /Workspace/utils/basic

# COMMAND ----------

from datetime import datetime, timedelta

volumepath = "/Volumes/mimi_ws_1/partcd/src/benefits/"

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

def get_latest(pattern, datethreshold = None):
    filepath_lst = sorted([filepath for filepath in Path(volumepath).rglob(pattern)], 
                      key=lambda x: x.stat().st_mtime, reverse=True)
    files_latest = {}
    for filepath in filepath_lst:
        tokens = str(filepath.parent.stem).split('-')
        mimi_src_file_name = filepath.parent.stem + '/' + filepath.name
        if 'quarter' in filepath.parent.stem and len(tokens) == 5:
            mimi_src_file_date = parse(f"{tokens[2]}-{int(tokens[-1])*3-2}-01").date()
        elif len(tokens) == 3:
            mimi_src_file_date = parse(f"{tokens[2]}-01-01").date()
        else:
            continue
        if mimi_src_file_date not in files_latest:
            if (datethreshold is None or
                mimi_src_file_date >= datethreshold):
                files_latest[mimi_src_file_date] = filepath
    return files_latest

# COMMAND ----------

def generic_ingestion(volumepath, filename, pdf_metadata, datethreshold = None):

    files_latest = get_latest(f"{filename}.txt", (datetime.today() - timedelta(days=365)).date())

    for mimi_src_file_date, filepath in files_latest.items():
        print(filepath)
        mimi_src_file_name = filepath.parent.stem + '/' + filepath.name
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

        mimi_src_file_date_str = mimi_src_file_date.strftime("%Y-%m-%d")        

        pdf = pdf.dropna(axis=1, how='all')
        df = spark.createDataFrame(pdf)
        (df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .option("mergeSchema", "true")
                .option("replaceWhere", f"mimi_src_file_date='{mimi_src_file_date_str}'")
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

files_latest = get_latest("*dictionary.xlsx", (datetime.today() - timedelta(days=365)).date())
for mimi_src_file_date, filepath in files_latest.items():
    mimi_src_file_name = filepath.parent.stem + '/' + filepath.name
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

# COMMAND ----------

pdf_metadata = spark.read.table('mimi_ws_1.partcd.pbp_metadata').toPandas()

# COMMAND ----------

volumepath = "/Volumes/mimi_ws_1/partcd/src/benefits/"

# COMMAND ----------

datethreshold = (datetime.today() - timedelta(days=365)).date()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section A

# COMMAND ----------

filename = "pbp_Section_A"
files_latest = get_latest(f"{filename}.txt", datethreshold)
for mimi_src_file_date, filepath in files_latest.items():
    print(filepath)
    mimi_src_file_name = "/".join(str(filepath).split('/')[-2:])
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
    pdf = pdf.dropna(axis=1, how='all')
    df = spark.createDataFrame(pdf)
    mimi_src_file_date_str = mimi_src_file_date.strftime('%Y-%m-%d')
    (df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("mergeSchema", "true")
            .option("replaceWhere", f"mimi_src_file_date = '{mimi_src_file_date_str}'")
            .saveAsTable(f"mimi_ws_1.partcd.{filename.lower()}"))
    #add_column_desc(filename, pdf_metadata)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part B Drug

# COMMAND ----------

filename = 'pbp_b15_partb_rx_drugs'
generic_ingestion(volumepath, filename, pdf_metadata, datethreshold)
#add_column_desc(filename, pdf_metadata)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section D

# COMMAND ----------

filename = 'pbp_Section_D'
generic_ingestion(volumepath, filename, pdf_metadata, datethreshold)
#add_column_desc(filename, pdf_metadata)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Supplemental Benefits

# COMMAND ----------

filename = 'pbp_Section_D_opt'
generic_ingestion(volumepath, filename, pdf_metadata, datethreshold)
#add_column_desc(filename, pdf_metadata)

# COMMAND ----------

# MAGIC %md
# MAGIC ## VBID

# COMMAND ----------

filename = 'pbp_b13i_b19b_services_vbid_ssbci'
generic_ingestion(volumepath, filename, pdf_metadata, datethreshold)
#add_column_desc(filename, pdf_metadata)

# COMMAND ----------

filename = 'pbp_b14c_b19b_preventive_vbid_uf'
generic_ingestion(volumepath, filename, pdf_metadata, datethreshold)
#add_column_desc(filename, pdf_metadata)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dental

# COMMAND ----------

filename = 'pbp_b16_dental'
generic_ingestion(volumepath, filename, pdf_metadata, datethreshold)
#add_column_desc(filename, pdf_metadata)

filename = 'pbp_b16_b19b_dental_vbid_uf'
generic_ingestion(volumepath, filename, pdf_metadata, datethreshold)
#add_column_desc(filename, pdf_metadata)

# COMMAND ----------

# MAGIC %md
# MAGIC ## MRX

# COMMAND ----------

filename = 'pbp_mrx'
generic_ingestion(volumepath, filename, pdf_metadata, datethreshold)
#add_column_desc(filename, pdf_metadata)

filename = 'pbp_mrx_vbid'
generic_ingestion(volumepath, filename, pdf_metadata, datethreshold)
#add_column_desc(filename, pdf_metadata)

filename = 'pbp_mrx_gapCoverage'
generic_ingestion(volumepath, filename, pdf_metadata, datethreshold)
#add_column_desc(filename, pdf_metadata)

filename = 'pbp_mrx_p'
generic_ingestion(volumepath, filename, pdf_metadata, datethreshold)
#add_column_desc(filename, pdf_metadata)

filename = 'pbp_mrx_p_vbid'
generic_ingestion(volumepath, filename, pdf_metadata, datethreshold)
#add_column_desc(filename, pdf_metadata)

filename = 'pbp_mrx_tier'
generic_ingestion(volumepath, filename, pdf_metadata, datethreshold)
#add_column_desc(filename, pdf_metadata)

filename = 'pbp_mrx_tier_vbid'
generic_ingestion(volumepath, filename, pdf_metadata, datethreshold)
#add_column_desc(filename, pdf_metadata)

# COMMAND ----------

# MAGIC %md
# MAGIC ## MISC

# COMMAND ----------

filename = 'pbp_b10_amb_trans'
generic_ingestion(volumepath, filename, pdf_metadata, datethreshold)
#add_column_desc(filename, pdf_metadata)

# COMMAND ----------

filename = 'pbp_b17_eye_exams_wear'
generic_ingestion(volumepath, filename, pdf_metadata, datethreshold)
#add_column_desc(filename, pdf_metadata)

filename = 'pbp_b17_b19b_eye_exams_wear_vbid_uf'
generic_ingestion(volumepath, filename, pdf_metadata, datethreshold)
#add_column_desc(filename, pdf_metadata)

# COMMAND ----------

filename = 'pbp_b18_hearing_exams_aids'
generic_ingestion(volumepath, filename, pdf_metadata, datethreshold)
#add_column_desc(filename, pdf_metadata)

filename = 'pbp_b18_b19b_hearing_exams_aids_vbid_uf'
generic_ingestion(volumepath, filename, pdf_metadata, datethreshold)
#add_column_desc(filename, pdf_metadata)

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_metadata IS '# [Medicare Advantage Benefits Information Metadata](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data), the list of tables, columns, and their descriptions. | resolution: variable, interval: quarterly
# MAGIC ';
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_section_a IS '# [Medicare Advantage Benefits Information Section A](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data), general plan information. | resolution: health_plan, interval: quarterly
# MAGIC ';
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_b13i_b19b_services_vbid_ssbci IS '# [Medicare Advantage Benefits Information Section B13I-B19B VBID](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data), Non-Primarily Health Related Benefits for the Chronically III, Food and Produce, Meals, Pest Control, Transportation for Non-Medical Needs, Indoor Air Quality Equipment and Services and Other data. | resolution: health_plan, interval: quarterly
# MAGIC ';
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_b14c_b19b_preventive_vbid_uf IS '# [Medicare Advantage Benefits Information Section B14C-B19B VBID](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data), Supplemental Benefits Preventive Services VBID and UF data for Health Education/Wellness, Fitness Benefit, Counseling Services. | resolution: health_plan, interval: quarterly
# MAGIC ';
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_b15_partb_rx_drugs IS '# [Medicare Advantage Benefits Information Section B15 PartB Rx](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data), Medicare Part B prescription drugs | resolution: health_plan, interval: quarterly
# MAGIC ';
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_section_d_opt IS '# [Medicare Advantage Benefits Information Section D Optional](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data), plan-level benefit data descriptions of optional supplemental offerings including the optional benefit premium amounts | resolution: health_plan, interval: quarterly
# MAGIC ';
# MAGIC
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_b16_dental IS '# [Medicare Advantage Benefits Information Section B16 Dental](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data), Preventive and Comprehensive Dental data | resolution: health_plan, interval: quarterly';
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_b16_b19b_dental_vbid_uf IS '# [Medicare Advantage Benefits Information Section B16-B19B Dental VBID UF](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data), Preventive and Comprehensive Dental VBID and UF data | resolution: health_plan, interval: quarterly';
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_mrx IS '# [Medicare Advantage Benefits Information Section MRX](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data), Medicare Part D prescription drug benefits data | resolution: health_plan, interval: quarterly';
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_mrx_vbid IS '# [Medicare Advantage Benefits Information Section MRX VBID](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data), Medicare Part D prescription drug benefits VBID data | resolution: health_plan, interval: quarterly';
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_mrx_gapCoverage IS '# [Medicare Advantage Benefits Information Section MRX Gap Coverage](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data), information on coverage of either tier(s) or specific drugs through the entire gap (ICL to catastrophic) | resolution: health_plan, interval: quarterly';
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_mrx_p IS '# [Medicare Advantage Benefits Information Section B14C-B19B MRX P](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data), Medicare Part D prescription drug benefits (post OOP threshold) data | resolution: health_plan, interval: quarterly';
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_mrx_p_vbid IS '# [Medicare Advantage Benefits Information Section MRX P VBID](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data), Medicare Part D prescription drug benefits (post OOP threshold) VBID data | resolution: health_plan, interval: quarterly';
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_mrx_tier IS '# [Medicare Advantage Benefits Information Section MRX TIER](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data), Medicare Part D prescription drug benefits (tiering) data | resolution: health_plan, interval: quarterly';
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_mrx_tier_vbid IS '# [Medicare Advantage Benefits Information Section MRX TIER VBID VBID](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data), Medicare Part D prescription drug benefits (tiering) data | resolution: health_plan, interval: quarterly';
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_b10_amb_trans IS '# [Medicare Advantage Benefits Information Section B10 Ambulance/Transportation data](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data), Ambulance/Transportation data benefits (tiering) data | resolution: health_plan, interval: quarterly';
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_section_d IS '# [Medicare Advantage Benefits Information Section D data](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data), plan-level financial data such as plan premiums, global max plan benefit and out-of-pocket limits, and global deductible data | resolution: health_plan, interval: quarterly';
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_b17_eye_exams_wear IS '# [Medicare Advantage Benefits Information Section B17 Eye Exams Wear data](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data), Eye Exams and Eye Wear data | resolution: health_plan, interval: quarterly';
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_b17_b19b_eye_exams_wear_vbid_uf IS '# [Medicare Advantage Benefits Information Section B17 B19b Eye Exams Wear VBID UF](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data), Eye Exams and Eye Wear VBID and UF data | resolution: health_plan, interval: quarterly';
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_b18_hearing_exams_aids IS '# [Medicare Advantage Benefits Information Section B18 Hearing Exams Aids data](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data), Hearing Exams and Hearing Aids data | resolution: health_plan, interval: quarterly';
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_b18_b19b_hearing_exams_aids_vbid_uf IS '# [Medicare Advantage Benefits Information Section B18 B19b Hearing Exams Aids VBID UF](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data), Hearing Exams and Hearing Aids VBID and UF data | resolution: health_plan, interval: quarterly';
# MAGIC */

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plan Areas

# COMMAND ----------

filename = "PlanArea"
files_latest = get_latest(f"{filename}.txt", datethreshold)

for mimi_src_file_date, filepath in files_latest.items():
    print(filepath)
    mimi_src_file_name = filepath.parent.stem + '/' + filepath.name
    pdf = pd.read_csv(filepath, sep="\t", encoding="ISO-8859-1", dtype=str,
                        quoting=csv.QUOTE_NONE)
    pdf['contract_year'] = pd.to_numeric(pdf['contract_year']).astype('int')
    pdf = pdf.dropna(axis=1, how='all')
    pdf['mimi_src_file_date'] = mimi_src_file_date
    pdf['mimi_src_file_name'] = mimi_src_file_name
    pdf['mimi_dlt_load_date'] = datetime.today().date()

    mimi_src_file_date_str = mimi_src_file_date.strftime("%Y-%m-%d")
    

    df = spark.createDataFrame(pdf)
    (df.write.mode("overwrite")
        .option('replaceWhere', f"mimi_src_file_date = '{mimi_src_file_date_str}'")
        .saveAsTable("mimi_ws_1.partcd.pbp_plan_area"))

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_plan_area IS '# [Medicare Advantage Benefits Information Section PlanArea Files](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data), Contains Service Area data by plan (includes EGHP service areas) | resolution: health_plan, interval: quarterly';
# MAGIC */

# COMMAND ----------

filename = "PlanRegionArea"
files_latest = get_latest(f"{filename}.txt", datethreshold)
for mimi_src_file_date, filepath in files_latest.items():
    print(filepath)
    mimi_src_file_name = filepath.parent.stem + '/' + filepath.name
    pdf = pd.read_csv(filepath, sep="\t", encoding="ISO-8859-1", dtype=str,
                        quoting=csv.QUOTE_NONE)
    pdf['contract_year'] = pd.to_numeric(pdf['contract_year']).astype('int')
    pdf = pdf.dropna(axis=1, how='all')
    pdf['mimi_src_file_date'] = mimi_src_file_date
    pdf['mimi_src_file_name'] = mimi_src_file_name
    pdf['mimi_dlt_load_date'] = datetime.today().date()
    mimi_src_file_date_str = mimi_src_file_date.strftime("%Y-%m-%d")
    df = spark.createDataFrame(pdf)
    (df.write.mode("overwrite")
        .option('replaceWhere', f"mimi_src_file_date = '{mimi_src_file_date_str}'")
        .saveAsTable("mimi_ws_1.partcd.pbp_plan_region_area"))

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.pbp_plan_region_area IS '# [Medicare Advantage Benefits Information Section PlanRegionArea Files](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data), Contains Service Region Area data by plan (Regional MA plans and PDPs) | resolution: health_plan, interval: quarterly';
# MAGIC */

# COMMAND ----------


