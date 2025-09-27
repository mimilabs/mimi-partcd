# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Overview
# MAGIC
# MAGIC This script downloads...
# MAGIC

# COMMAND ----------

# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/download_utils

# COMMAND ----------

!pip install beautifulsoup4 tqdm

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect files to download

# COMMAND ----------

import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime

# COMMAND ----------

url_base = "https://www.cms.gov"
page = "/data-research/statistics-trends-reports/medicare-advantage/part-d-contract-and-enrollment-data/mpf-benefit-summary-report"
volumepath = "/Volumes/mimi_ws_1/partcd/src/mpf_benefit_summary"

# COMMAND ----------

response = requests.get(f"{url_base}{page}")
response.raise_for_status()  # This will raise an error if the fetch fails
soup = BeautifulSoup(response.text, 'html.parser')

# COMMAND ----------

current_year = datetime.today().year

# COMMAND ----------

table = soup.find('table')
pages = []
for row in table.find_all('tr'):
    td_lst = [td for td in row.find_all('td')]
    if len(td_lst) != 2:
        continue
    year = int(td_lst[-1].text.strip()[-4:])
    if year < current_year - 2:
        continue
    if row.find('a', href=True):
        pages.append(row.find('a')['href'])

# COMMAND ----------

for page in pages:
    response = requests.get(f"{url_base}{page}")
    response.raise_for_status()  # This will raise an error if the fetch fails
    soup = BeautifulSoup(response.text, 'html.parser')
    download_links = [url_base + a['href'] for a in soup.find_all('a', href=True)
                        if a['href'].endswith('.zip')]
    download_files(download_links, volumepath + '/downloads')


# COMMAND ----------

files_downloaded = [x for x in Path(volumepath + '/downloads').glob("*.zip")
                    if (datetime.today() - datetime.fromtimestamp(x.stat().st_mtime)).days < 90]

# COMMAND ----------

for path_zip in files_downloaded:
    path_unzip = (str(path_zip.parents[1]) + '/' + str(path_zip.stem))
    unzip(path_zip, path_unzip)

# COMMAND ----------


