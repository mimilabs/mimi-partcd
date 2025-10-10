# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/text_utils

# COMMAND ----------

import pyspark.sql.functions as F
from collections import Counter

volumepath = "/Volumes/mimi_ws_1/partcd/src/landscape/"
#target_filename = "CY2025_Landscape_202409.csv"
target_filename = "CY2026_Landscape_202509.csv"
target_year = int(target_filename[2:6])
mimi_src_file_date = parse(f"{target_year}-01-01").date()

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

# NOTE: organization_name has too many variations and typos
# We clean up the values to align over the years

orgname_sourcetable = 'mimi_ws_1.partcd.landscape_plan_premium_report'
orgname_sourcetable = 'mimi_ws_1.partcd.landscape_medicare_advantage'

orgname_dict = Counter({x['organization_name']:x['count'] 
                        for x in (spark.read.table(orgname_sourcetable)
                        .groupBy('organization_name')
                        .count().collect())})
orgname2stdname = {}
stdname2orgname = {}
stdname_cnt = Counter()
for k, v in orgname_dict.items():
    stdname = standardize_entity(k).lower()
    stdname_cnt[stdname] = stdname_cnt.get(stdname, 0) + v
    orgname2stdname[k] = stdname
    if stdname not in stdname2orgname:
        stdname2orgname[stdname] = Counter()
    
    stdname2orgname[stdname][k] = v
stdname2orgname = {k: v.most_common(1)[0][0] for k, v in stdname2orgname.items()}
orgname_remap = create_typo_mapping(stdname_cnt, min_dist=2, 
                                    ignore_lst=['and',
                                                'inc',
                                                'care',
                                                'group',
                                                ' '])

# COMMAND ----------

orgname_remap_v2 = {}
for k, v in orgname2stdname.items():
    stdname = orgname2stdname.get(v, v)
    stdname = orgname_remap.get(stdname, stdname)
    orgname_remap_v2[k] = stdname2orgname.get(stdname, k)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Premium Info

# COMMAND ----------

map_common = {
    'state_territory_name': 'state',
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

pdf = pd.read_csv(f"{volumepath}{target_filename}", dtype=str)
pdf.columns = change_header(pdf.columns)
pdf = pdf.rename(columns=map_common)
pdf = pdf.rename(columns={    
    'parent_organization_name': 'organization_name',
    'annual_part_d_deductible_amount': 'part_d_drug_deductible',
    'drug_benefit_type': 'benefit_type',
})
pdf['plan_type'] = pdf['plan_type'].replace(plan_type_remap)
pdf['organization_name'] = pdf['organization_name'].replace(orgname_remap_v2)

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
pdf['mimi_src_file_date'] = mimi_src_file_date
pdf['mimi_src_file_name'] = target_filename
pdf['mimi_dlt_load_date'] = datetime.today().date()
df = spark.createDataFrame(pdf)
(df.write.mode('overwrite')
    .option('mergeSchema', 'true')
    .option('replaceWhere', f"mimi_src_file_name = '{target_filename}'")
    .saveAsTable('mimi_ws_1.partcd.landscape_plan_premium_report'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Break down by contract category
# MAGIC This is to match with the old (legacy) formats

# COMMAND ----------

pdf = pd.read_csv(f"{volumepath}{target_filename}", dtype=str)
pdf.columns = change_header(pdf.columns)
pdf = pdf.rename(columns=map_common)
pdf = pdf.rename(columns={    
    'parent_organization_name': 'organization_name',
    'plan_type': 'type_of_medicare_health_plan',
    'annual_part_d_deductible_amount': 'annual_drug_deductible',
})
pdf['organization_name'] = pdf['organization_name'].replace(orgname_remap_v2)

# COMMAND ----------

pdf.groupby('contract_category_type').count()

# COMMAND ----------

pdf_ma = pdf.loc[pdf['contract_category_type'].isin(['MA', 'MAPD', 'MA-PD']),:].copy()

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
pdf['mimi_src_file_date'] = mimi_src_file_date
pdf['mimi_src_file_name'] = target_filename
pdf['mimi_dlt_load_date'] = datetime.today().date()
df = spark.createDataFrame(pdf)
(df.write.mode('overwrite')
    .option('mergeSchema', 'true')
    .option('replaceWhere', f"mimi_src_file_name = '{target_filename}'")
    .saveAsTable('mimi_ws_1.partcd.landscape_medicare_advantage'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## PDP

# COMMAND ----------

pdf = pd.read_csv(f"{volumepath}{target_filename}", dtype=str)
pdf.columns = change_header(pdf.columns)
pdf = pdf.rename(columns=map_common)
pdf = pdf.rename(columns={    
    'parent_organization_name': 'company_name',
    'drug_benefit_type': 'benefit_type',
    'annual_part_d_deductible_amount': 'annual_drug_deductible',
    'part_d_total_premium': 'monthly_drug_premium'
})
pdf['plan_type'] = pdf['plan_type'].replace(plan_type_remap)

# COMMAND ----------

pdf_pdp = pdf.loc[pdf['contract_category_type'].isin(['PDP']),:].copy()

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
pdf['mimi_src_file_date'] = mimi_src_file_date
pdf['mimi_src_file_name'] = target_filename
pdf['mimi_dlt_load_date'] = datetime.today().date()
df = spark.createDataFrame(pdf)
(df.write.mode('overwrite')
    .option('mergeSchema', 'true')
    .option('replaceWhere', f"mimi_src_file_name = '{target_filename}'")
    .saveAsTable('mimi_ws_1.partcd.landscape_prescription_drug_plan'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## SNP

# COMMAND ----------

pdf = pd.read_csv(f"{volumepath}{target_filename}", dtype=str)
pdf.columns = change_header(pdf.columns)
pdf = pdf.rename(columns=map_common)
pdf = pdf.rename(columns={    
    'part_d_total_premium': 'monthly_drug_premium',
    'parent_organization_name': 'organization_name',
     'plan_type': 'type_of_medicare_health_plan',
     'annual_part_d_deductible_amount': 'annual_drug_deductible',
})
pdf['organization_name'] = pdf['organization_name'].replace(orgname_remap_v2)

# COMMAND ----------

pdf_snp = pdf.loc[pdf['contract_category_type'].isin(['SNP']),:].copy()

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
pdf['mimi_src_file_date'] = mimi_src_file_date
pdf['mimi_src_file_name'] = target_filename
pdf['mimi_dlt_load_date'] = datetime.today().date()
df = spark.createDataFrame(pdf)
(df.write.mode('overwrite')
    .option('mergeSchema', 'true')
    .option('replaceWhere', f"mimi_src_file_name = '{target_filename}'")
    .saveAsTable('mimi_ws_1.partcd.landscape_special_needs_plan'))

# COMMAND ----------


