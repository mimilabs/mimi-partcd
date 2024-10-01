# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/download_utils

# COMMAND ----------

!pip install beautifulsoup4 tqdm

# COMMAND ----------

import requests
from bs4 import BeautifulSoup
import re

url_base = "https://www.cms.gov"
volumepath = "/Volumes/mimi_ws_1/partcd/src/starrating/zipfiles"

# COMMAND ----------

page = "/medicare/health-drug-plans/part-c-d-performance-data"
response = requests.get(f"{url_base}{page}")
response.raise_for_status()  # This will raise an error if the fetch fails
soup = BeautifulSoup(response.text, 'html.parser')

# COMMAND ----------

urls = []
for a in soup.find_all('a', href=True):
    if (a.get('href').endswith('.zip') and
        ('Ratings' in a.text or
         'Measures' in a.text)):
        urls.append(url_base + a.get('href'))

# COMMAND ----------

download_files(urls, volumepath)

# COMMAND ----------

for path_zip in Path(volumepath).glob('*.zip'):
    path_unzip = (str(path_zip.parents[1]) + '/' + str(path_zip.stem))
    unzip(path_zip, path_unzip)
    for path_zip_child in Path(path_unzip).glob('*.zip'):
        path_unzip_child = (str(path_zip_child.parent) + '/' + str(path_zip_child.stem))
        unzip(path_zip_child, path_unzip_child)

# COMMAND ----------


