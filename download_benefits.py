# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Overview
# MAGIC
# MAGIC This script downloads...
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect files to download

# COMMAND ----------

# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/download_utils

# COMMAND ----------

import datetime
from dateutil.relativedelta import *
import math

# COMMAND ----------

url = "https://www.cms.gov/files/zip"
url2 = "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/pbp-pufs"
volumepath = "/Volumes/mimi_ws_1/partcd/src"
volumepath_zip = f"{volumepath}/zipfiles"
retrieval_range = 120 # in months

# COMMAND ----------

ref_monthyear = datetime.datetime.now() + relativedelta(months=3) 
files_to_download = {} # an array of tuples
for mon_diff in range(0, retrieval_range): 
    target_date = (ref_monthyear - relativedelta(months=mon_diff))
    year = target_date.year 
    quarter = int(math.ceil(target_date.month/3.0))
    if year == 2024 and quarter == 2:
        continue
    elif year==2022 and quarter == 4:
        continue
    elif year == 2021 and quarter == 4:
        continue
    elif year == 2020 and quarter == 3:
        continue
    
    generic_name = f"pbp-benefits-{year}-quarter-{quarter}.zip"
    if year == 2022 and quarter == 3:
        files_to_download[generic_name] = url + "/pbp-benefits-2022-updated-07012022.zip" 
    elif year == 2021 and quarter == 1:
        files_to_download[generic_name] = url + "/pbp-benefits-2021-01122021.zip"
    elif year == 2021 and quarter == 2:
        files_to_download[generic_name] = url + "/pbp-benefits-2021-04012021.zip"
    elif year == 2021 and quarter == 3:
        files_to_download[generic_name] = url + "/pbp-benefits-2021-updated-08022021.zip"
    elif year == 2021 and quarter == 1:
        files_to_download[generic_name] = url2 + f"/pbp-benefits-{year}-q{quarter}.zip"
    elif year == 2020 and quarter in {2, 4}:
         files_to_download[generic_name] = url + "/" + generic_name
    elif year < 2021 and year >= 2018:
        files_to_download[generic_name] = url2 + f"/pbp-benefits-{year}-q{quarter}.zip"
    elif year < 2018:
        generic_name = f"pbp-benefits-{year}-quarter-1.zip"
        files_to_download[generic_name] = url2 + f"/pbp-benefits-{year}.zip"
    else:    
        files_to_download[generic_name] = url + "/" + generic_name


# COMMAND ----------

url_pairs = [(v, k) for k, v in files_to_download.items()]
urls = [pair[0] for pair in url_pairs]
filenames = [pair[1] for pair in url_pairs]

# COMMAND ----------

download_files(urls, volumepath_zip, filenames)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unzip the downloaded files

# COMMAND ----------

for filepath in Path(volumepath_zip).glob("pbp-*.zip"):
    unzip(filepath, volumepath + f"/benefits/{filepath.stem}")
    

# COMMAND ----------


