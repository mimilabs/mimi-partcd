# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the HEDIS files
# MAGIC

# COMMAND ----------

!pip install openpyxl

# COMMAND ----------

from pathlib import Path
import re
from dateutil.parser import parse
import pandas as pd
import numpy as np
path = "/Volumes/mimi_ws_1/partcd/src/" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "partcd" # delta table destination schema
tablename = "hedis" # destination table

# COMMAND ----------

def camel_to_underscore(text):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', text).lower()

def change_header(header_org):
    return [camel_to_underscore(re.sub(r'\W+', '', column))
            for column in header_org]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Years 2019 ~ Present

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE mimi_ws_1.partcd.hedis_measures;
# MAGIC --DROP TABLE mimi_ws_1.partcd.hedis_rau_measures;

# COMMAND ----------

files = [
    (2023, "HEDIS2023.xlsx", "MADictionary2023.xlsx"),
    (2022, "HEDIS2022_updated082023.xlsx", "MADictionary2022.xlsx"),
    (2021, "HEDIS2021_updated082023.xlsx", "MADictionary2021.xlsx"),
    (2019, "HEDIS2019.xlsx", "MADictionary2019.xlsx")
]

# COMMAND ----------

general_colmap = {"general0010": "organization_type",
	"general0011": "plan_type",
	"general0014": "snp_offered",
	"general0015":	"partd_offered",
 	"general0016":	"partd_offered",
	"general0020":	"line_of_business",
	"general0050":	"enrollment_yearend",
	"general0060":	"cms_region_number",
	"general0070":	"cms_region_name",
 	"general_0070":	"cms_region_name",
	"general0080":	"population_type",
	"general0085":	"sumitted_to_ncqa",
	"general0087":	"hos_included"}
national_colmap = {
    "n_r0010": "hedis_year",	
	"n_r0020": "measure_name",
	"n_r0030": "indicator_description",
	"n_r0040": "indicator_key",
	"n_r0050": "rate",
	"n_r0060": "num_reporting_contracts",
	"n_r0070": "tot_num_enrollees"
}

# COMMAND ----------

hedis_lst = []
for item in files:
    
    hedis_lst_sub = []

    madict = pd.read_excel(f"{path}/hedis/{item[2]}")
    madict.columns = change_header(madict.columns)
    madict = madict.loc[:,
                        ["measure_code", 
                         "indicator_key", 
                         "measure_name", 
                         "short_indicator_name"]].drop_duplicates()

    hedis_measures = pd.read_excel(f"{path}/hedis/{item[1]}", 
                                   sheet_name = "hedis_measures")
    hedis_measures.columns = change_header(hedis_measures.columns)
    hedis_measures = hedis_measures.rename(columns={
                            "c_m_s_contract_number": "contract_number"})
    hedis_lst_sub.append(hedis_measures)
    
    # 2019, 2021, 2022, 2023
    hedishos_frm = pd.read_excel(f"{path}/hedis/{item[1]}", 
                                    sheet_name = "hedishos_frm")
    hedishos_frm.columns = change_header(hedishos_frm.columns)
    hedishos_frm["measure_code"] = "FRM"
    hedishos_frm1 = hedishos_frm.loc[:,['contract_number', 'measure_code', 'rate1']]
    hedishos_frm2 = hedishos_frm.loc[:,['contract_number', 'measure_code', 'rate2']]
    hedishos_frm1 = hedishos_frm1.rename(columns={"rate1": "rate"})
    hedishos_frm2 = hedishos_frm2.rename(columns={"rate2": "rate"})
    hedishos_frm1["indicator_key"] = "discussing_fall_risk_rate"
    hedishos_frm2["indicator_key"] = "managing_fall_risk_rate"
    hedishos_frm1["lcl"] = np.nan
    hedishos_frm2["lcl"] = np.nan
    hedishos_frm1["ucl"] = np.nan
    hedishos_frm2["ucl"] = np.nan
    hedis_lst_sub.append(hedishos_frm1)
    hedis_lst_sub.append(hedishos_frm2)

    hedishos_pao = pd.read_excel(f"{path}/hedis/{item[1]}", 
                                    sheet_name = "hedishos_pao")
    hedishos_pao.columns = change_header(hedishos_pao.columns)
    hedishos_pao["measure_code"] = "PAO-HOS"
    hedishos_pao1 = hedishos_pao.loc[:,['contract_number', 'measure_code', 'rate1']]
    hedishos_pao2 = hedishos_pao.loc[:,['contract_number', 'measure_code', 'rate2']]
    hedishos_pao1 = hedishos_pao1.rename(columns={"rate1": "rate"})
    hedishos_pao2 = hedishos_pao2.rename(columns={"rate2": "rate"})
    hedishos_pao1["indicator_key"] = "discussing_physical_activity_rate"
    hedishos_pao2["indicator_key"] = "advising_physical_activity_rate"
    hedishos_pao1["lcl"] = np.nan
    hedishos_pao2["lcl"] = np.nan
    hedishos_pao1["ucl"] = np.nan
    hedishos_pao2["ucl"] = np.nan
    hedis_lst_sub.append(hedishos_pao1)
    hedis_lst_sub.append(hedishos_pao2)

    if item[0] > 2020:
        hedishos_mui = pd.read_excel(f"{path}/hedis/{item[1]}", 
                                        sheet_name = "hedishos_mui")
        hedishos_mui.columns = change_header(hedishos_mui.columns)
        hedishos_mui["measure_code"] = "MUI-HOS"
        hedishos_mui1 = hedishos_mui.loc[:,['contract_number', 'measure_code', 'rate1']]
        hedishos_mui2 = hedishos_mui.loc[:,['contract_number', 'measure_code', 'rate2']]
        hedishos_mui3 = hedishos_mui.loc[:,['contract_number', 'measure_code', 'rate3']]
        hedishos_mui1 = hedishos_mui1.rename(columns={"rate1": "rate"})
        hedishos_mui2 = hedishos_mui2.rename(columns={"rate2": "rate"})
        hedishos_mui3 = hedishos_mui3.rename(columns={"rate3": "rate"})
        hedishos_mui1["indicator_key"] = "discussing_urinary_incontinence_rate"
        hedishos_mui2["indicator_key"] = "treatment_of_urinary_incontinence_rate"
        hedishos_mui3["indicator_key"] = "impact_of_urinary_incontinence_rate"
        hedishos_mui1["lcl"] = np.nan
        hedishos_mui2["lcl"] = np.nan
        hedishos_mui3["lcl"] = np.nan
        hedishos_mui1["ucl"] = np.nan
        hedishos_mui2["ucl"] = np.nan
        hedishos_mui3["ucl"] = np.nan
        hedis_lst_sub.append(hedishos_mui1)
        hedis_lst_sub.append(hedishos_mui2)
        hedis_lst_sub.append(hedishos_mui3)

    if item[0] < 2022:
        hedishos_oto = pd.read_excel(f"{path}/hedis/{item[1]}", 
                                        sheet_name = "hedishos_oto")
        hedishos_oto.columns = change_header(hedishos_oto.columns)
        hedishos_oto["measure_code"] = "OTO-HOS"
        hedishos_oto = hedishos_oto.loc[:,['contract_number', 'measure_code', 'rate']]
        hedishos_oto["indicator_key"] = "osteoporosis_testing_rate"
        hedishos_oto["lcl"] = np.nan
        hedishos_oto["ucl"] = np.nan
        hedis_lst_sub.append(hedishos_oto)
    
    hedis_pdf_sub = pd.concat(hedis_lst_sub)
    hedis_pdf_sub["hedis_year"] = item[0]
    hedis_pdf_sub = pd.merge(hedis_pdf_sub, madict, 
                             on=["measure_code", "indicator_key"], 
                             how="left")
    hedis_lst.append(hedis_pdf_sub)

hedis_pdf = pd.concat(hedis_lst)

# COMMAND ----------

hedis_pdf["rate_nq"] = hedis_pdf["rate"] == "NQ"
hedis_pdf["rate_nr"] = hedis_pdf["rate"] == "NR"
hedis_pdf["rate_nb"] = hedis_pdf["rate"] == "NB"
hedis_pdf["rate_br"] = hedis_pdf["rate"] == "BR"
hedis_pdf["rate"] = pd.to_numeric(hedis_pdf["rate"], errors="coerce")
hedis_pdf["lcl"] = pd.to_numeric(hedis_pdf["lcl"], errors="coerce")
hedis_pdf["ucl"] = pd.to_numeric(hedis_pdf["ucl"], errors="coerce")
hedis_pdf["numerator"] = pd.to_numeric(hedis_pdf["numerator"], errors="coerce")
hedis_pdf["denominator"] = pd.to_numeric(hedis_pdf["denominator"], errors="coerce")

df = spark.createDataFrame(hedis_pdf)
(df.write
    .format('delta')
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.hedis_measures"))

# COMMAND ----------

hedis_rau_lst = []
for item in files:
    if item[0] < 2020:
        continue
    madict = pd.read_excel(f"{path}/hedis/{item[2]}")
    madict.columns = change_header(madict.columns)
    madict = madict.loc[:,["measure_code", 
                         "indicator_key", 
                         "measure_name", 
                         "short_indicator_name"]].drop_duplicates()
    hedis_rau_measures = pd.read_excel(f"{path}/hedis/{item[1]}", 
                                       sheet_name = "hedis_rau_measures")
    hedis_rau_measures.columns = change_header(hedis_rau_measures.columns)
    hedis_rau_measures = hedis_rau_measures.rename(columns={"c_m_s_contract_number": "contract_number"})
    hedis_rau_measures["hedis_year"] = item[0]
    hedis_rau_measures = pd.merge(hedis_rau_measures, madict, 
                             on=["measure_code", "indicator_key"], 
                             how="left")
    hedis_rau_lst.append(hedis_rau_measures)
    
hedis_rau_pdf = pd.concat(hedis_rau_lst)
hedis_rau_pdf["denominator"] = pd.to_numeric(hedis_rau_pdf["denominator"], errors="coerce")
hedis_rau_pdf["observed_count"] = pd.to_numeric(hedis_rau_pdf["observed_count"], errors="coerce")
hedis_rau_pdf["expected_count"] = pd.to_numeric(hedis_rau_pdf["expected_count"], errors="coerce")
hedis_rau_pdf["member_count"] = pd.to_numeric(hedis_rau_pdf["member_count"], errors="coerce")
hedis_rau_pdf["outlier_member_count"] = pd.to_numeric(hedis_rau_pdf["outlier_member_count"], 
                                                      errors="coerce")
hedis_rau_pdf["non_outlier_member_count"] = pd.to_numeric(
    hedis_rau_pdf["non_outlier_member_count"], errors="coerce")
hedis_rau_pdf["count_variance"] = pd.to_numeric(hedis_rau_pdf["count_variance"], 
                                                errors="coerce")

# COMMAND ----------

df = spark.createDataFrame(hedis_rau_pdf)
(df.write
    .format('delta')
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.hedis_rau_measures"))

# COMMAND ----------

general_lst = []
national_lst = []
for item in files:
    general = pd.read_excel(f"{path}/hedis/{item[1]}", 
                            sheet_name = "general",
                            dtype={"General-0060": str})
    general.columns = [general_colmap.get(x, x) for x in change_header(general.columns)]
    general = general.rename(columns={"c_m_s_contract_number": "contract_number"})
    general["hedis_year"] = item[0]
    general_lst.append(general)

    national_rates = pd.read_excel(f"{path}/hedis/{item[1]}", 
                                   sheet_name = "national_rates")
    national_rates.columns = [national_colmap.get(x, x) 
                              for x in change_header(national_rates.columns)]
    national_rates = national_rates.rename(columns={"measure_name": "measure_code"})
    national_lst.append(national_rates)
    

# COMMAND ----------

general_pdf = pd.concat(general_lst)
df = spark.createDataFrame(general_pdf)
(df.write
    .format('delta')
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.hedis_general"))

# COMMAND ----------

national_pdf = pd.concat(national_lst)
df = spark.createDataFrame(national_pdf)
(df.write
    .format('delta')
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.hedis_national_rates"))

# COMMAND ----------


