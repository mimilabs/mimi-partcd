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

!pip install bs4

# COMMAND ----------

import datetime
from dateutil.relativedelta import *
import math

# COMMAND ----------

from bs4 import BeautifulSoup
import requests
import json
import time
from datetime import datetime

volumepath = "/Volumes/mimi_ws_1/partcd/src"
volumepath_zip = f"{volumepath}/zipfiles"
url_base = "https://www.cms.gov"
page_start = "/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/benefits-data"
response = requests.get(url_base + page_start)
soup = BeautifulSoup(response.content, 'html.parser')

# COMMAND ----------

current_year = datetime.today().year

# COMMAND ----------

pages = []
for tr in soup.find("table").find("tbody").find_all("tr"):
    td_lst = tr.find_all("td")
    if len(td_lst) != 2:
        continue
    year = int(td_lst[-1].text.strip()[-4:])
    if year < current_year - 1:
        continue
    pages.append(tr.find("a").get("href"))

# COMMAND ----------

download_urls = []
for page in pages:
    response = requests.get(url_base + page)
    soup = BeautifulSoup(response.content, 'html.parser')
    li = soup.find('li', class_="field__item")
    if li is None:
        continue
    download_url = url_base + li.find('a').get('href')
    download_urls.append(download_url)

# COMMAND ----------

download_files(download_urls, volumepath_zip)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unzip the downloaded files

# COMMAND ----------

folders_existing = {x.name for x in Path(volumepath + "/benefits").glob("*")}

# COMMAND ----------

for filepath in Path(volumepath_zip).glob("pbp-*.zip"):
    if filepath.stem in folders_existing:
        continue
    unzip(filepath, volumepath + f"/benefits/{filepath.stem}")

# COMMAND ----------


