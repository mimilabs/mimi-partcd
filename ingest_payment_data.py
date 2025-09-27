# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

volumepath = "/Volumes/mimi_ws_1/partcd/src/payment"

# COMMAND ----------

files_exist = {
    x[0] for x in (spark.read.table("mimi_ws_1.partcd.partc_payment_county_level")
                   .select('mimi_src_file_name')
                   .distinct()
                   .collect())
}

# COMMAND ----------

for filepath in Path(volumepath).rglob("*PartCCountyLevel.xlsx"):
    mimi_src_file_date = parse(filepath.stem[:4]+'-12-31').date()
    mimi_src_file_name = filepath.parent.stem + '/' + filepath.name
    if mimi_src_file_name in files_exist:
        continue

    excel_file = pd.ExcelFile(filepath)
    sheet_names_list = excel_file.sheet_names
    sheet_num = 0
    if len(sheet_names_list) > 1: 
        sheet_num = next((i for i, name in enumerate(sheet_names_list) if name.startswith("result")), 0)
    
    if filepath.stem[:4] == "2011": # exception
        pdf = pd.read_excel(filepath, dtype=str, header=None)
    else:
        pdf = pd.read_excel(filepath, dtype=str, skiprows=2, sheet_name=sheet_num)
            
    header = ["county_code", "state", "county_name",
                   "plan_type", "snp_plan_type", "avg_risk_score",
                   "avg_ab_pmpm_payment",
                   "avg_rebate_pmpm_payment"]
    if len(pdf.columns) == 9:
        header += ["avg_msa_monthly_deposit_payment"]
    pdf.columns = header
    pdf['county_code'] = pdf['county_code'].str.zfill(5)
    for col in pdf.columns:
        if col.endswith("payment") or col.endswith("score"):
            pdf[col] = pd.to_numeric(pdf[col], errors='coerce').astype(float)
    pdf['mimi_src_file_date'] = mimi_src_file_date
    pdf['mimi_src_file_name'] = mimi_src_file_name
    pdf['mimi_dlt_load_date'] = datetime.today().date()
    (
        spark.createDataFrame(pdf)
            .write
            .mode('overwrite')
            .option("replaceWhere", f"mimi_src_file_name = '{mimi_src_file_name}'")
            .saveAsTable("mimi_ws_1.partcd.partc_payment_county_level")
    )

# COMMAND ----------

files_exist = {
    x[0] for x in (spark.read.table("mimi_ws_1.partcd.partc_payment_plan_level")
                   .select('mimi_src_file_name')
                   .distinct()
                   .collect())
}

# COMMAND ----------

for filepath in Path(volumepath).rglob("*PartCPlan*Level.xlsx"):
    mimi_src_file_date = parse(filepath.stem[:4]+'-12-31').date()
    mimi_src_file_name = filepath.parent.stem + '/' + filepath.name
    if mimi_src_file_name in files_exist:
        continue
    excel_file = pd.ExcelFile(filepath)
    sheet_names_list = excel_file.sheet_names
    sheet_num = 0
    if len(sheet_names_list) > 1: 
        sheet_num = next((i for i, name in enumerate(sheet_names_list) if name.startswith("result")), 0)
    pdf = pd.read_excel(filepath, dtype=str, skiprows=2, sheet_name=sheet_num)
    header = ["contract_number", "plan_benefit_package",
                   "contract_name", "plan_type", "avg_partc_risk_score",
                   "avg_ab_pmpm_payment",
                   "avg_rebate_pmpm_payment"]
    if len(pdf.columns) == 8:
        header += ["avg_msa_monthly_deposit_payment"]
    pdf.columns = header
    for col in pdf.columns:
        if col.endswith("payment") or col.endswith("score"):
            pdf[col] = pd.to_numeric(pdf[col], errors='coerce').astype(float)
    pdf['mimi_src_file_date'] = mimi_src_file_date
    pdf['mimi_src_file_name'] = mimi_src_file_name
    pdf['mimi_dlt_load_date'] = datetime.today().date()
    (
        spark.createDataFrame(pdf)
            .write
            .mode('overwrite')
            .option("replaceWhere", f"mimi_src_file_name = '{mimi_src_file_name}'")
            .saveAsTable("mimi_ws_1.partcd.partc_payment_plan_level")
    )

# COMMAND ----------

files_exist = {
    x[0] for x in (spark.read.table("mimi_ws_1.partcd.partd_payment_plan_level")
                   .select('mimi_src_file_name')
                   .distinct()
                   .collect())
}

# COMMAND ----------

for filepath in Path(volumepath).rglob("*PartDPlans.xlsx"):
    mimi_src_file_date = parse(filepath.stem[:4]+'-12-31').date()
    mimi_src_file_name = filepath.parent.stem + '/' + filepath.name
    if mimi_src_file_name in files_exist:
        continue
    excel_file = pd.ExcelFile(filepath)
    sheet_names_list = excel_file.sheet_names
    sheet_num = 0
    if len(sheet_names_list) > 1: 
        sheet_num = next((i for i, name in enumerate(sheet_names_list) if name.startswith("result")), 0)
    pdf = pd.read_excel(filepath, dtype=str, skiprows=2, sheet_name=sheet_num)
    header = ["contract_number", "plan_benefit_package",
              "contract_name", "organization_type",
              "avg_partd_direct_subsidy_pmpm_payment",
              "avg_partd_risk_score",
              "avg_reinsurance_pmpm_payment",
              "avg_low_income_cost_sharing_pmpm_payment"]
    pdf.columns = header
    for col in pdf.columns:
        if col.endswith("payment") or col.endswith("score"):
            pdf[col] = pd.to_numeric(pdf[col], errors='coerce').astype(float)
    pdf['mimi_src_file_date'] = mimi_src_file_date
    pdf['mimi_src_file_name'] = mimi_src_file_name
    pdf['mimi_dlt_load_date'] = datetime.today().date()
    (
        spark.createDataFrame(pdf)
            .write
            .mode('overwrite')
            .option("replaceWhere", f"mimi_src_file_name = '{mimi_src_file_name}'")
            .saveAsTable("mimi_ws_1.partcd.partd_payment_plan_level")
    )

# COMMAND ----------

files_exist = {
    x[0] for x in (spark.read.table("mimi_ws_1.partcd.partd_reconciliation_contract_level")
                   .select('mimi_src_file_name')
                   .distinct()
                   .collect())
}

# COMMAND ----------

file_lst = [x for x in Path(volumepath).rglob("*PartD Reconciliation.xlsx")]
file_lst += [x for x in Path(volumepath).rglob("*PartDReconcilia*.xlsx")]

# COMMAND ----------

for filepath in file_lst:
    mimi_src_file_date = parse('20'+filepath.stem[2:4]+'-12-31').date()
    mimi_src_file_name = filepath.parent.stem + '/' + filepath.name
    if mimi_src_file_name in files_exist:
        continue
    pdf = pd.read_excel(filepath, dtype=str, sheet_name=0)
    pdf.columns = ["contract_number", "contract_name",
                   "number_of_plans", "reconciliation_amount",
                   "lics_amount", "reinsurance_amount", "risk_sharing_amount"]
    pdf = pdf.loc[pdf["contract_number"]!="Totals"]
    for col in pdf.columns:
        if col.endswith("amount"):
            pdf[col] = pd.to_numeric(pdf[col], errors='coerce').astype(float)
    pdf['mimi_src_file_date'] = mimi_src_file_date
    pdf['mimi_src_file_name'] = mimi_src_file_name
    pdf['mimi_dlt_load_date'] = datetime.today().date()
    (
        spark.createDataFrame(pdf)
            .write
            .mode('overwrite')
            .option("replaceWhere", f"mimi_src_file_name = '{mimi_src_file_name}'")
            .saveAsTable("mimi_ws_1.partcd.partd_reconciliation_contract_level")
    )

# COMMAND ----------

for filepath in file_lst:
    mimi_src_file_date = parse('20'+filepath.stem[2:4]+'-12-31').date()
    mimi_src_file_name = filepath.parent.stem + '/' + filepath.name
    if mimi_src_file_name in files_exist:
        continue
    pdf = pd.read_excel(filepath, dtype=str, sheet_name=1)
    pdf.columns = ["parent_organization_name", "number_of_contracts",
                   "reconciliation_amount",
                   "lics_amount", "reinsurance_amount", "risk_sharing_amount"]
    pdf = pdf.loc[pdf["parent_organization_name"]!="Totals"]
    for col in pdf.columns:
        if col.endswith("amount"):
            pdf[col] = pd.to_numeric(pdf[col], errors='coerce').astype(float)
    pdf['mimi_src_file_date'] = mimi_src_file_date
    pdf['mimi_src_file_name'] = mimi_src_file_name
    pdf['mimi_dlt_load_date'] = datetime.today().date()
    (
        spark.createDataFrame(pdf)
            .write
            .mode('overwrite')
            .option("replaceWhere", f"mimi_src_file_name = '{mimi_src_file_name}'")
            .saveAsTable("mimi_ws_1.partcd.partd_reconciliation_parent_level")
    )

# COMMAND ----------


