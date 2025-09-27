# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/download_utils

# COMMAND ----------

!pip install beautifulsoup4 tqdm

# COMMAND ----------

import requests
from bs4 import BeautifulSoup
import re

url_base = "https://www.cms.gov"
volumepath = "/Volumes/mimi_ws_1/partcd/src/directory/downloads"

# COMMAND ----------

page = "/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/ma-plan-directory"
response = requests.get(f"{url_base}{page}")
response.raise_for_status()  # This will raise an error if the fetch fails
soup = BeautifulSoup(response.text, 'html.parser')

# COMMAND ----------

p = soup.find('div', class_='rxbodyfield').find_all('p')[-1]
p.find('strong').decompose()
report_period = p.text.strip()
li = soup.find('li', class_="field__item")
download_link = li.find('a').get('href')
download_url = url_base + download_link
download_path = Path(download_url)
filename = download_path.stem + '-' + report_period + download_path.suffix

# COMMAND ----------

download_file(download_url, volumepath, filename)

# COMMAND ----------

for path_zip in Path(volumepath).glob(f'{filename}*'):
    path_unzip = (str(path_zip.parents[1]) + '/' + str(path_zip.stem))
    unzip(path_zip, path_unzip)

# COMMAND ----------

# MAGIC %md
# MAGIC ## PDP

# COMMAND ----------

page = "/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/pdp-plan-directory"
response = requests.get(f"{url_base}{page}")
response.raise_for_status()  # This will raise an error if the fetch fails
soup = BeautifulSoup(response.text, 'html.parser')

# COMMAND ----------

p = soup.find('div', class_='rxbodyfield').find_all('p')[-1]
p.find('strong').decompose()
report_period = p.text.strip()
li = soup.find('li', class_="field__item")
download_link = li.find('a').get('href')
download_url = url_base + download_link
download_path = Path(download_url)
filename = download_path.stem + '-' + report_period + download_path.suffix

# COMMAND ----------

download_file(download_url, volumepath, filename)

# COMMAND ----------

for path_zip in Path(volumepath).glob(f'{filename}*'):
    path_unzip = (str(path_zip.parents[1]) + '/' + str(path_zip.stem))
    unzip(path_zip, path_unzip)

# COMMAND ----------


