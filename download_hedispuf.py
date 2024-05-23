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

!pip install tqdm

# COMMAND ----------

import requests
from pathlib import Path
from tqdm import tqdm
import datetime
from dateutil.relativedelta import *
import zipfile

# COMMAND ----------

url1 = "https://www.cms.gov/files/zip" # 2019 ~ present
url2 = "https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/ma-hedis-pufs" # 1997 ~ 2018
volumepath = "/Volumes/mimi_ws_1/partcd/src"
volumepath_zip = f"{volumepath}/zipfiles"

# COMMAND ----------

files_to_download = []
for year in range(1997, 2019):
    if year == 2012:
        files_to_download.append((f"{url2}/hedis-pufs-{year}-exl.zip",
                              f"ma-hedis-pufs-{year}.zip"))
    else:
        files_to_download.append((f"{url2}/ma-hedis-pufs-{year}-exl.zip",
                              f"ma-hedis-pufs-{year}.zip"))
for year in range(2019, 2024):
    if year == 2019:
        files_to_download.append((f"{url1}/hedis-ma-puf-{year}.zip",
                              f"ma-hedis-pufs-{year}.zip"))
    elif year == 2020:
        continue
    else:
        files_to_download.append((f"{url1}/ma-hedis-pufs-{year}.zip",
                              f"ma-hedis-pufs-{year}.zip"))

# COMMAND ----------

def download_file(url, filename, folder):
    # NOTE the stream=True parameter below
    with requests.get(f"{url}", stream=True) as r:
        r.raise_for_status()
        with open(f"{folder}/{filename}", 'wb') as f:
            for chunk in tqdm(r.iter_content(chunk_size=8192)): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)

# COMMAND ----------

# Display the zip file links
for filename_tuple in files_to_download:
    # Check if the file exists
    if Path(f"{volumepath_zip}/{filename_tuple[1]}").exists():
        # print(f"{filename} exists, skipping...")
        continue
    else:
        print(f"{filename_tuple[0]} downloading...")
        download_file(filename_tuple[0], filename_tuple[1], volumepath_zip)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unzip the downloaded files

# COMMAND ----------

files_downloaded = [x for x in Path(volumepath_zip).glob("ma-hedis-*.zip")]

# COMMAND ----------

for file_downloaded in files_downloaded:
    with zipfile.ZipFile(file_downloaded, "r") as zip_ref:
        for member in zip_ref.namelist():
            if not Path(f"{volumepath}/hedis/{member}").exists():
                print(f"Extracting {member}...")
                zip_ref.extract(member, path=f"{volumepath}/hedis")

# COMMAND ----------


