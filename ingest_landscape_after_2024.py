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

pdf = pd.read_csv(f"{volumepath}CY2025_Landscape_202409.csv", dtype=str)
pdf.columns = change_header(pdf.columns)
pdf = pdf.rename(columns={'state_name': 'state',
        'county_name': 'county', 
        'parent_organization_name': 'organization_name',
        'special_needs_plan_snp_indicator': 'special_needs_plan',
        'snp_type': 'special_needs_plan_type',
        'drug_benefit_type': 'benefit_type',
        'part_d_basic_premium_at_or_below_regional_benchmark': 'part_d_basic_premium_below_regional_benchmark',
        'offers_drug_tier_with_no_part_d_deductible':'tiers_not_subject_to_deductible',
        'part_d_low_income_beneficiary_premium_amount': 'part_d_premium_obligation_with_full_premium_assistance',
        'annual_part_d_deductible_amount': 'part_d_drug_deductible',
        'part_d_outof_pocket_oop_threshold': 'increased_initial_coverage_limit'
                    })

# COMMAND ----------

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



# COMMAND ----------

df.display()

# COMMAND ----------


