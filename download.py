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

url = "https://www.cms.gov/files/zip"
volumepath = "/Volumes/mimi_ws_1/partcd/src"
volumepath_zip = f"{volumepath}/zipfiles"
retrieval_range = 48 # in months

# COMMAND ----------

ref_monthyear = datetime.datetime.now()
files_to_download = [] # an array of tuples
for mon_diff in range(1, retrieval_range): 
    monthyear = (ref_monthyear - relativedelta(months=mon_diff)).strftime('%B-%Y').lower()
    if monthyear == "july-2023":
        files_to_download.append(("monthlycpsc202307.zip",
                                 f"monthly-enrollment-cpsc-{monthyear}.zip"))
    else:
        files_to_download.append((f"monthly-enrollment-cpsc-{monthyear}.zip",
                                  f"monthly-enrollment-cpsc-{monthyear}.zip"))

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
        download_file(f"{url}/{filename_tuple[0]}", filename_tuple[1], volumepath_zip)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unzip the downloaded files

# COMMAND ----------

files_downloaded = [x for x in Path(volumepath_zip).glob("monthly-enrollment-cpsc-*.zip")]

# COMMAND ----------

for file_downloaded in files_downloaded:
    with zipfile.ZipFile(file_downloaded, "r") as zip_ref:
        for member in zip_ref.namelist():
            if not Path(f"{volumepath}/cpsc/{member}").exists():
                print(f"Extracting {member}...")
                zip_ref.extract(member, path=f"{volumepath}/cpsc")

# COMMAND ----------


