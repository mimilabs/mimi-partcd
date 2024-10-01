# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Premium Info

# COMMAND ----------

from collections import defaultdict

# COMMAND ----------

volumepath = "/Volumes/mimi_ws_1/partcd/src/landscape/"

def str_to_num(value):
    # Remove spaces and the dollar sign
    if pd.isna(value):
        return None
    cleaned = value.replace("$", "").strip()
    
    # Check if the value is in parentheses (negative)
    sign = 1
    if cleaned.startswith("(") and cleaned.endswith(")"):
        sign = -1
    cleaned = cleaned.strip('()')
    try:
        return float(cleaned)
    except ValueError:
        return None

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS mimi_ws_1.partcd.landscape_plan_premium_report;

# COMMAND ----------


filepath_lst = sorted([filepath for filepath in Path(volumepath).glob("*_Plan_Premium_Report_*")], reverse=True)
renames = {'part_c_premium2': 'part_c_premium',
        'part_c_premium_2': 'part_c_premium',
        'part_d_basic_premium3': 'part_d_basic_premium',
        'part_d_supplemental_premium4': 'part_d_supplemental_premium',
        'part_d_total_premium5': 'part_d_total_premium',
        'part_d_premium_obligation_with_full_premium_assistance6': 'part_d_premium_obligation_with_full_premium_assistance',
        'part_d_premium_obligation_with_full_premium_assistance_6': 'part_d_premium_obligation_with_full_premium_assistance',
        'tiers_not_subject_to_deductible7': 'tiers_not_subject_to_deductible',
        'increased_initial_coverage_limit8': 'increased_initial_coverage_limit',
        'part_d_supplemental_premium_4': 'part_d_supplemental_premium',
        '2024_star_rating_part_c': 'star_rating_part_c',
        '2024_star_rating_part_d': 'star_rating_part_d',
        '2024_overall_star_rating': 'overall_star_rating'
        }
numeric_cols = ['part_c_premium',
                'part_d_basic_premium',
                'part_d_supplemental_premium',
                'part_d_total_premium',
                'part_d_premium_obligation_with_full_premium_assistance',
                'part_d_premium_obligation_with_75_premium_assistance',
                'part_d_premium_obligation_with_50_premium_assistance',
                'part_d_premium_obligation_with_25_premium_assistance',
                'part_d_drug_deductible',
                'star_rating_part_c',
                'star_rating_part_d,'
                'overall_star_rating']
pdf_lst = []
cols = defaultdict(list)
for filepath in filepath_lst:
    mimi_src_file_name = filepath.name
    mimi_src_file_date = parse(filepath.stem[2:6] + '-01-01').date()
    skiprows = 0
    if filepath.stem[-8:] < '20230701':
        skiprows = 3
    pdf = pd.read_csv(filepath, encoding='ISO-8859-1', skiprows=skiprows, dtype=str)
    pdf.columns = change_header(pdf.columns)
    pdf.rename(columns=renames, inplace=True)
    drop_cols = []
    for col in pdf.columns:
        if col.startswith('unnamed'):
            drop_cols.append(col)
        elif col in numeric_cols:
            pdf[col] = pdf[col].apply(lambda x: str_to_num(x))
    if len(drop_cols) > 0:
        pdf.drop(columns=drop_cols, inplace=True)
    pdf['mimi_src_file_date'] = mimi_src_file_date
    pdf['mimi_src_file_name'] = mimi_src_file_name
    pdf['mimi_dlt_load_date'] = datetime.today().date()
    pdf_lst.append(pdf)
df = spark.createDataFrame(pd.concat(pdf_lst))

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("mimi_ws_1.partcd.landscape_plan_premium_report")

# COMMAND ----------

# MAGIC %md
# MAGIC ## MA
# MAGIC

# COMMAND ----------

filepath_lst = sorted([filepath for filepath in Path(volumepath).glob("*_MA_*")], reverse=True)


# COMMAND ----------

renames = {'2024_overall_star_rating': 'overall_star_rating'}
numeric_cols = ['monthly_consolidated_premium_includes_part_c_d',
                'annual_drug_deductible',
                'overall_star_rating',
                'innetwork_moop_amount']
pdf_lst = []
for filepath in filepath_lst:
    mimi_src_file_name = filepath.name
    mimi_src_file_date = parse(filepath.stem[2:6] + '-01-01').date()
    skiprows = 0
    if filepath.stem[-8:] < '20230701':
        skiprows = 5
    pdf = pd.read_csv(filepath, encoding='ISO-8859-1', skiprows=skiprows, dtype=str)
    pdf.columns = change_header(pdf.columns)
    pdf.rename(columns=renames, inplace=True)
    drop_cols = []
    for col in pdf.columns:
        if col.startswith('unnamed'):
            drop_cols.append(col)
        elif col in numeric_cols:
            pdf[col] = pdf[col].apply(lambda x: str_to_num(x))
    if len(drop_cols) > 0:
        pdf.drop(columns=drop_cols, inplace=True)
    pdf['mimi_src_file_date'] = mimi_src_file_date
    pdf['mimi_src_file_name'] = mimi_src_file_name
    pdf['mimi_dlt_load_date'] = datetime.today().date()
    pdf_lst.append(pdf)
df = spark.createDataFrame(pd.concat(pdf_lst))

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("mimi_ws_1.partcd.landscape_medicare_advantage")

# COMMAND ----------

# MAGIC %md
# MAGIC ## pdp

# COMMAND ----------

filepath_lst = sorted([filepath for filepath in Path(volumepath).glob("*_PDP_*")], reverse=True)


# COMMAND ----------

renames = {'2024_star_rating_part_d': 'summary_star_rating',
           'organization_name': 'company_name'}
numeric_cols = ['monthly_drug_premium',
                'annual_drug_deductible',
                'summary_star_rating']
pdf_lst = []
for filepath in filepath_lst:
    mimi_src_file_name = filepath.name
    mimi_src_file_date = parse(filepath.stem[2:6] + '-01-01').date()
    skiprows = 0
    if filepath.stem[-8:] < '20230701':
        skiprows = 3
    pdf = pd.read_csv(filepath, encoding='ISO-8859-1', skiprows=skiprows, dtype=str)
    pdf.columns = change_header(pdf.columns)
    pdf.rename(columns=renames, inplace=True)
    drop_cols = []
    for col in pdf.columns:
        if col.startswith('unnamed'):
            drop_cols.append(col)
        elif col in numeric_cols:
            pdf[col] = pdf[col].apply(lambda x: str_to_num(x))
    if len(drop_cols) > 0:
        pdf.drop(columns=drop_cols, inplace=True)
    pdf['mimi_src_file_date'] = mimi_src_file_date
    pdf['mimi_src_file_name'] = mimi_src_file_name
    pdf['mimi_dlt_load_date'] = datetime.today().date()
    pdf_lst.append(pdf)
df = spark.createDataFrame(pd.concat(pdf_lst))

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("mimi_ws_1.partcd.landscape_prescription_drug_plan")

# COMMAND ----------

# MAGIC %md
# MAGIC ## snp

# COMMAND ----------

filepath_lst = sorted([filepath for filepath in Path(volumepath).glob("*_SNP_*")], reverse=True)


# COMMAND ----------

renames = {'2024_overall_star_rating': 'overall_star_rating',
           'drug_benefit_type1': 'drug_benefit_type_detail'}
numeric_cols = ['monthly_consolidated_premium_includes_part_c_d',
                'annual_drug_deductible',
                'overall_star_rating']
pdf_lst = []
for filepath in filepath_lst:
    mimi_src_file_name = filepath.name
    mimi_src_file_date = parse(filepath.stem[2:6] + '-01-01').date()
    skiprows = 0
    if filepath.stem[-8:] == '20221014':
        skiprows = 6
    elif filepath.stem[-8:] < '20221014':
        skiprows = 4
    pdf = pd.read_csv(filepath, encoding='ISO-8859-1', skiprows=skiprows, dtype=str)
    pdf.columns = change_header(pdf.columns)
    pdf.rename(columns=renames, inplace=True)
    drop_cols = []
    for col in pdf.columns:
        if col.startswith('unnamed'):
            drop_cols.append(col)
        elif col in numeric_cols:
            pdf[col] = pdf[col].apply(lambda x: str_to_num(x))
    if len(drop_cols) > 0:
        pdf.drop(columns=drop_cols, inplace=True)
    pdf['mimi_src_file_date'] = mimi_src_file_date
    pdf['mimi_src_file_name'] = mimi_src_file_name
    pdf['mimi_dlt_load_date'] = datetime.today().date()
    pdf_lst.append(pdf)
df = spark.createDataFrame(pd.concat(pdf_lst))

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("mimi_ws_1.partcd.landscape_special_needs_plan")

# COMMAND ----------


