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

# MAGIC %md
# MAGIC ## Collect files to download

# COMMAND ----------

import datetime
from dateutil.relativedelta import *

# COMMAND ----------

url_base = "https://www.cms.gov/files/zip"
volumepath = "/Volumes/mimi_ws_1/partcd/src"
volumepath_zip = f"{volumepath}/zipfiles"
retrieval_range = 24 # in months

# COMMAND ----------

ref_monthyear = datetime.datetime.now()
files_to_download = [] # an array of tuples
urls = []
filenames = []
for mon_diff in range(0, retrieval_range): 
    target_dt = (ref_monthyear - relativedelta(months=mon_diff))
    monthyear = target_dt.strftime('%B-%Y').lower()
    target_dt_str = target_dt.strftime('%Y_%m')
    filename = f"monthly-enrollment-cpsc-{monthyear}.zip"
    if monthyear == "july-2023":
        download_file(url_base + "/monthlycpsc202307.zip", 
                      volumepath_zip, 
                      filename)                      
    elif monthyear == "november-2019":
        download_file(url_base + "/cpsc-entrollment-2019-11.zip", 
                      volumepath_zip, 
                      filename)
    elif monthyear == "january-2017":
        download_file("https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/2017/jan/cpsc_enrollment_2017_01.zip",
                      volumepath_zip,
                      filename)
    elif monthyear == "december-2016":
        download_file('https://www.cms.gov/files/zip/cpscenrollment201612zip',
                      volumepath_zip,
                      filename)
    elif target_dt_str < "2015_09":
        folderpath = target_dt.strftime('%Y/%b').lower()
        folderpath = (folderpath.replace('/sep', '/sept')
                        .replace('/jul', '/july'))
        download_file(f"https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/{folderpath}/cpsc-enrollment-{target_dt_str.replace('_','-')}.zip",
                      volumepath_zip,
                      filename)
    elif target_dt_str < "2015_10":
        folderpath = target_dt.strftime('%Y/%B').lower()
        download_file(f"https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/{folderpath}/cpsc-enrollment-{target_dt_str.replace('_','-')}.zip",
                      volumepath_zip,
                      filename)
    elif target_dt_str < "2016_03":
        folderpath = target_dt.strftime('%Y/%b').lower()
        download_file(f"https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/{folderpath}/cpsc-enrollment-{target_dt_str.replace('_','-')}.zip",
                      volumepath_zip,
                      filename)
    elif target_dt_str < "2016_09":
        folderpath = target_dt.strftime('%Y/%B').lower()
        if folderpath == "2016/april":
            folderpath = "2016/apr"        
        download_file(f"https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/{folderpath}/cpsc-enrollment-{target_dt_str.replace('_','-')}.zip",
                      volumepath_zip,
                      filename)
    elif target_dt_str < "2016_12":
        folderpath = target_dt.strftime('%Y/%b').lower()
        folderpath = folderpath.replace('/sep', '/sept')
        download_file(f"https://www.cms.gov/research-statistics-data-and-systems/statistics-trends-and-reports/mcradvpartdenroldata/downloads/{folderpath}/cpsc-enrollment-{target_dt_str.replace('_','-')}.zip",
                      volumepath_zip,
                      filename)
    elif target_dt_str < "2019_11":
        download_file(
            f"https://downloads.cms.gov/files/cpsc_enrollment_{target_dt_str}.zip",
            volumepath_zip,
            filename
            )
    else:
        download_file(url_base + "/" + filename, 
                      volumepath_zip, 
                      filename) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unzip the downloaded files

# COMMAND ----------

files_downloaded = [x for x in Path(volumepath_zip).glob("monthly-enrollment-cpsc-*.zip")]

# COMMAND ----------

for path_zip in files_downloaded:
    path_unzip = (str(path_zip.parents[1]) + "/cpsc")
    unzip(path_zip, path_unzip)
