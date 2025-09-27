# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/download_utils

# COMMAND ----------

!pip install beautifulsoup4 tqdm

# COMMAND ----------

import requests
from bs4 import BeautifulSoup
import re

url_base = "https://www.cms.gov"
volumepath = "/Volumes/mimi_ws_1/partcd/src/landscape/downloads"

# COMMAND ----------

page = "/medicare/coverage/prescription-drug-coverage"
response = requests.get(f"{url_base}{page}")
response.raise_for_status()  # This will raise an error if the fetch fails
soup = BeautifulSoup(response.text, 'html.parser')

# COMMAND ----------

download_links = []
for li in soup.find_all('li', class_="field__item"):
    download_links.append(url_base + li.find('a').get('href').strip())

# COMMAND ----------

download_files(download_links, volumepath)

# COMMAND ----------

for filepath_zip in Path(volumepath).glob("*.zip*"):
    unzip(filepath_zip, filepath_zip.parent)
    

# COMMAND ----------


