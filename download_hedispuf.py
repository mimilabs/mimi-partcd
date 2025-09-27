# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/download_utils

# COMMAND ----------

!pip install tqdm bs4

# COMMAND ----------

import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime

url_base = "https://www.cms.gov"
volumepath = "/Volumes/mimi_ws_1/partcd/src"
volumepath_zip = f"{volumepath}/zipfiles"

# COMMAND ----------

page = "/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/ma-hedis-public-use-files"
response = requests.get(f"{url_base}{page}")
response.raise_for_status()  # This will raise an error if the fetch fails
soup = BeautifulSoup(response.text, 'html.parser')

# COMMAND ----------

current_year = datetime.today().year

# COMMAND ----------

pages = []
for tr in soup.find("table").find("tbody").find_all("tr"):
    td_lst = tr.find_all("td")
    if len(td_lst) != 2:
        continue
    year = int(td_lst[-1].text.strip()[-4:])
    if year < current_year - 3:
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

files_downloaded = [x for x in Path(volumepath_zip).glob("ma-hedis-*.zip")
                    if (datetime.today() - datetime.fromtimestamp(x.stat().st_mtime)).days < 90]

# COMMAND ----------

for file_downloaded in files_downloaded:
    unzip(file_downloaded, str(file_downloaded.parents[1]) + '/hedis')

# COMMAND ----------


