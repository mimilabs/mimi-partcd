# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Premium Info

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

map_common = {
    'state_name': 'state',
    'county_name': 'county', 
    'special_needs_plan_snp_indicator': 'special_needs_plan',
    'snp_type': 'special_needs_plan_type',
    'part_d_basic_premium_at_or_below_regional_benchmark': 'part_d_basic_premium_below_regional_benchmark',
    'offers_drug_tier_with_no_part_d_deductible':'tiers_not_subject_to_deductible',
    'part_d_low_income_beneficiary_premium_amount': 'part_d_premium_obligation_with_full_premium_assistance',
    'part_d_outof_pocket_oop_threshold': 'increased_initial_coverage_limit',
    "in_network_maximum_outof_pocket_moop_amount": "innetwork_moop_amount",
    "monthly_consolidated_premium_part_c_d": "monthly_consolidated_premium_includes_part_c_d",
    'low_income_subsidy_lis_auto_enrollment': '0_premium_with_full_low_income_subsidy',
    'dual_eligible_snp_dsnp_integration_status': 'integration_status',
    'dsnp_applicable_integrated_plan_aip_identifier': 'aip_status',
}
plan_type_remap = {'HMO C-SNP': 'HMO',
 'HMO D-SNP': 'HMO', 
 'HMO I-SNP': 'HMO',
 'HMO-POS': 'HMOPOS', 
 'HMO-POS C-SNP': 'HMOPOS', 
 'HMO-POS D-SNP': 'HMOPOS', 
 'HMO-POS I-SNP': 'HMOPOS', 
 'PPO': 'Local PPO',
 'PPO C-SNP': 'Local PPO',
 'PPO D-SNP': 'Local PPO',
 'PPO I-SNP': 'Local PPO',
 'Regional PPO C-SNP': 'Regional PPO',
 'Regional PPO D-SNP': 'Regional PPO',
 'PDP': 'Medicare Prescription Drug Plan', 
 'Cost': '1876 Cost'}

# COMMAND ----------

pdf = pd.read_csv(f"{volumepath}CY2025_Landscape_202409.csv", dtype=str)
pdf.columns = change_header(pdf.columns)
pdf = pdf.rename(columns=map_common)
pdf = pdf.rename(columns={    
    'parent_organization_name': 'organization_name',
    'annual_part_d_deductible_amount': 'part_d_drug_deductible',
    'drug_benefit_type': 'benefit_type',
})

# COMMAND ----------

pdf['plan_type'] = pdf['plan_type'].replace(plan_type_remap)

# COMMAND ----------

dtypes = spark.read.table('mimi_ws_1.partcd.landscape_plan_premium_report').dtypes
selected_columns = []
for col, dtype in dtypes:
    if col not in pdf.columns:
        print(col, '*')
    else:
        if dtype == 'double':
            pdf[col] = pdf[col].apply(str_to_num)
        selected_columns.append(col)

# COMMAND ----------

pdf = pdf[selected_columns]
pdf['mimi_src_file_date'] = parse('2025-01-01').date()
pdf['mimi_src_file_name'] = 'CY2025_Landscape_202409.csv'
pdf['mimi_dlt_load_date'] = datetime.today().date()
df = spark.createDataFrame(pdf)
(df.write.mode('overwrite')
    .option('mergeSchema', 'true')
    .option('replaceWhere', "mimi_src_file_name = 'CY2025_Landscape_202409.csv'")
    .saveAsTable('mimi_ws_1.partcd.landscape_plan_premium_report'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Break down by contract category

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

pdf = pd.read_csv(f"{volumepath}CY2025_Landscape_202409.csv", dtype=str)
pdf.columns = change_header(pdf.columns)
pdf = pdf.rename(columns=map_common)
pdf = pdf.rename(columns={    
    'parent_organization_name': 'organization_name',
    'plan_type': 'type_of_medicare_health_plan',
    'annual_part_d_deductible_amount': 'annual_drug_deductible',
})

# COMMAND ----------

pdf_ma = pdf.loc[pdf['contract_category_type'].isin(['MA', 'MAPD']),:]

# COMMAND ----------

dtypes = spark.read.table('mimi_ws_1.partcd.landscape_medicare_advantage').dtypes
selected_columns = []
for col, dtype in dtypes:
    if col not in pdf_ma.columns:
        print(col, '*')
    else:
        if dtype == 'double':
            pdf_ma[col] = pdf_ma[col].apply(str_to_num)
        selected_columns.append(col)

# COMMAND ----------

pdf = pdf_ma[selected_columns]
pdf['mimi_src_file_date'] = parse('2025-01-01').date()
pdf['mimi_src_file_name'] = 'CY2025_Landscape_202409.csv'
pdf['mimi_dlt_load_date'] = datetime.today().date()
df = spark.createDataFrame(pdf)
(df.write.mode('overwrite')
    .option('mergeSchema', 'true')
    .option('replaceWhere', "mimi_src_file_name = 'CY2025_Landscape_202409.csv'")
    .saveAsTable('mimi_ws_1.partcd.landscape_medicare_advantage'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## PDP

# COMMAND ----------

pdf = pd.read_csv(f"{volumepath}CY2025_Landscape_202409.csv", dtype=str)
pdf.columns = change_header(pdf.columns)
pdf = pdf.rename(columns=map_common)
pdf = pdf.rename(columns={    
    'parent_organization_name': 'company_name',
    'drug_benefit_type': 'benefit_type',
    'annual_part_d_deductible_amount': 'annual_drug_deductible',
    'part_d_total_premium': 'monthly_drug_premium'
})

# COMMAND ----------

pdf_pdp = pdf.loc[pdf['contract_category_type'].isin(['PDP']),:]

# COMMAND ----------

dtypes = spark.read.table('mimi_ws_1.partcd.landscape_prescription_drug_plan').dtypes
selected_columns = []
for col, dtype in dtypes:
    if col not in pdf_pdp.columns:
        print(col, '*')
    else:
        if dtype == 'double':
            pdf_pdp[col] = pdf_pdp[col].apply(str_to_num)
        selected_columns.append(col)

# COMMAND ----------

pdf = pdf_pdp[selected_columns]
pdf['mimi_src_file_date'] = parse('2025-01-01').date()
pdf['mimi_src_file_name'] = 'CY2025_Landscape_202409.csv'
pdf['mimi_dlt_load_date'] = datetime.today().date()
df = spark.createDataFrame(pdf)
(df.write.mode('overwrite')
    .option('mergeSchema', 'true')
    .option('replaceWhere', "mimi_src_file_name = 'CY2025_Landscape_202409.csv'")
    .saveAsTable('mimi_ws_1.partcd.landscape_prescription_drug_plan'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## SNP

# COMMAND ----------

pdf = pd.read_csv(f"{volumepath}CY2025_Landscape_202409.csv", dtype=str)
pdf.columns = change_header(pdf.columns)
pdf = pd.read_csv(f"{volumepath}CY2025_Landscape_202409.csv", dtype=str)
pdf.columns = change_header(pdf.columns)
pdf = pdf.rename(columns=map_common)
pdf = pdf.rename(columns={    
    'part_d_total_premium': 'monthly_drug_premium',
    'parent_organization_name': 'organization_name',
     'plan_type': 'type_of_medicare_health_plan',
     'annual_part_d_deductible_amount': 'annual_drug_deductible',
})

# COMMAND ----------

pdf_snp = pdf.loc[pdf['contract_category_type'].isin(['SNP']),:]

# COMMAND ----------

dtypes = spark.read.table('mimi_ws_1.partcd.landscape_special_needs_plan').dtypes
selected_columns = []
for col, dtype in dtypes:
    if col not in pdf_snp.columns:
        print(col, '*')
    else:
        if dtype == 'double':
            pdf_snp[col] = pdf_snp[col].apply(str_to_num)
        selected_columns.append(col)

# COMMAND ----------

pdf = pdf_snp[selected_columns]
pdf['mimi_src_file_date'] = parse('2025-01-01').date()
pdf['mimi_src_file_name'] = 'CY2025_Landscape_202409.csv'
pdf['mimi_dlt_load_date'] = datetime.today().date()
df = spark.createDataFrame(pdf)
(df.write.mode('overwrite')
    .option('mergeSchema', 'true')
    .option('replaceWhere', "mimi_src_file_name = 'CY2025_Landscape_202409.csv'")
    .saveAsTable('mimi_ws_1.partcd.landscape_special_needs_plan'))
