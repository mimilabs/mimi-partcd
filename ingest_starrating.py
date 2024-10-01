# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the HEDIS files
# MAGIC

# COMMAND ----------

# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

path = "/Volumes/mimi_ws_1/partcd/src/starrating" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "partcd" # delta table destination schema

# COMMAND ----------

def convert_to_num(x):
    if isinstance(x, float):
        return x
    else:
        try:
            return float(x.replace('%',''))
        except ValueError:
            return None

# COMMAND ----------

def parse_file(filepath):
    pdf = pd.read_csv(filepath, encoding='ISO-8859-1', dtype=str, header=None)
    perf_year = int(str(filepath).split('/')[6][:4])
    plan_info_cols = pdf.iloc[1,:].tolist()
    measure_heading = pdf.iloc[2,:].tolist()
    measure_period = pdf.iloc[3,:].tolist()
    cols = []
    for i, (heading, period) in enumerate(zip(measure_heading, measure_period)):
        if all(pd.isna(c) for c in [plan_info_cols[i], heading, period]):
            continue
        if pd.isna(period):
            measure_key = format_header_varname(plan_info_cols[i]).replace('contract_number', 
                                                                           'contract_id')
            col = (i, measure_key, None, None, None, None)
            cols.append(col)
            continue
        measure_start = None
        measure_end = None
        period = period.replace('(Monday - Friday)','').replace("\x96", "").strip()
        try:
            measure_start = parse(period.split()[0]).date()
            measure_end = parse(period.split()[-1]).date()
        except ParserError:
            print(heading, period)

        measure_code = heading.split(':')[0].strip()
        measure_desc = format_header_varname(heading.split(':')[-1])
        
        col = (i, 'measure', measure_code, measure_desc, measure_start, measure_end)
        cols.append(col)

    data = []    
    for i, row in pdf.iloc[4:,:].iterrows():
        if all(pd.isna(x) for x in row):
            continue
        d_base = []
        d_measures = []
        for col in cols:
            if col[1] == 'measure':
                d_measures.append([col[2], 
                                col[3], 
                                col[4], 
                                col[5], 
                                row[col[0]].strip()])
            else:
                d_base.append(row[col[0]].strip())
        for d_measure in d_measures:
            data.append(d_base + d_measure)
    pdf = pd.DataFrame(data, columns=['contract_id', 
                                     'organization_type', 
                                     'contract_name', 
                                     'organization_marketing_name', 
                                     'parent_organization',
                                     'measure_code', 
                                     'measure_desc', 
                                     'measure_start', 
                                     'measure_end',
                                     'measure_value_raw'])
    
    mimi_src_file_date = None
    if 'Measure Data (' in filepath.stem:
        mimi_src_file_date = parse(filepath.stem.split('(')[1].split(')')[0]).date()
    elif filepath.stem.endswith('_data'):
        mimi_src_file_date = parse(filepath.stem[-15:-5].replace('_','-')).date()
    elif 'Measure Stars (' in filepath.stem:
        if '(Jul 2 2024)' in filepath.stem:
            mimi_src_file_date = parse('2024-07-02').date()
        else:
            mimi_src_file_date = parse(filepath.stem.split('(')[1].split(')')[0]).date()
    elif filepath.stem.endswith('_stars'):
        mimi_src_file_date = parse(filepath.stem[-15:-5].replace('_','-')).date()

    assert mimi_src_file_date is not None

    pdf['measure_value'] = pdf['measure_value_raw'].apply(convert_to_num)
    pdf['performance_year'] = perf_year
    pdf['mimi_src_file_date'] = mimi_src_file_date
    pdf['mimi_src_file_name'] = filepath.name
    pdf['mimi_dlt_load_date'] = datetime.today().date()

    return pdf

# COMMAND ----------

# MAGIC %md
# MAGIC ## Values

# COMMAND ----------

file_lst = [filepath for filepath in Path(path).rglob("*Master[_ ]Table*data.csv")
            if str(filepath).split('/')[6] not in {'2007_plan_ratings',
                                    '2008_plan_ratings',
                                    '2009_plan_ratings',
                                    '2010_plan_ratings_and_display_measures',
                                    '2011_plan_ratings_and_display_measures',
                                    '2012_plan_ratings_and_display_measures',
                                    '2013_plan_ratings_and_display_measures'}]
file_lst += [filepath for filepath in Path(path).rglob("*Measure Data*.csv")]
file_lst = sorted(file_lst, reverse=True)

# COMMAND ----------

pdf_lst = []
for filepath in file_lst:
    pdf_lst.append(parse_file(filepath))
pdf_full = pd.concat(pdf_lst)
df = spark.createDataFrame(pdf_full)

# COMMAND ----------

df.write.mode('overwrite').saveAsTable('mimi_ws_1.partcd.starrating_measure_value')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stars

# COMMAND ----------

file_lst = [filepath for filepath in Path(path).rglob("*Master[_ ]Table*stars.csv")
            if str(filepath).split('/')[6] not in {'2007_plan_ratings',
                                    '2008_plan_ratings',
                                    '2009_plan_ratings',
                                    '2010_plan_ratings_and_display_measures',
                                    '2011_plan_ratings_and_display_measures',
                                    '2012_plan_ratings_and_display_measures',
                                    '2013_plan_ratings_and_display_measures'}]
file_lst += [filepath for filepath in Path(path).rglob("*Measure Stars*.csv")]
file_lst = sorted(file_lst, reverse=True)

# COMMAND ----------

pdf_lst = []
for filepath in file_lst:
    pdf_lst.append(parse_file(filepath))
pdf_full = pd.concat(pdf_lst)
df = spark.createDataFrame(pdf_full)

# COMMAND ----------

df.write.mode('overwrite').saveAsTable('mimi_ws_1.partcd.starrating_measure_star')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Displays and HEDIS

# COMMAND ----------

def parse_display_file(filepath):
    offset = 1
    if 'HEDIS' in filepath.stem:
        offset = 0
    pdf = pd.read_csv(filepath, encoding='ISO-8859-1', dtype=str, header=None)
    perf_year = int(str(filepath).split('/')[6][:4])
    plan_info_cols = pdf.iloc[1,:].tolist()
    measure_heading = pdf.iloc[1+offset,:].tolist()
    
    cols = []
    for i, heading in enumerate(measure_heading):
        if all(pd.isna(c) for c in [plan_info_cols[i], heading]):
            continue
        if i < 4:
            measure_key = (format_header_varname(plan_info_cols[i])
                                .replace('contract_number', 'contract_id'))
            col = (i, measure_key, None, None)
            cols.append(col)
            continue

        heading_tokens = heading.split()
        measure_code = heading_tokens[-1].strip()
        measure_desc = format_header_varname(' '.join(heading_tokens[0:-1]))
        col = (i, 'measure', measure_code, measure_desc)
        cols.append(col)
    data = []    
    for i, row in pdf.iloc[(2+offset):,:].iterrows():
        if all(pd.isna(x) for x in row):
            continue
        d_base = []
        d_measures = []
        for col in cols:
            if col[1] == 'measure':
                d_measures.append([col[2], 
                                col[3], 
                                row[col[0]].strip()])
            else:
                d_base.append(row[col[0]].strip())
        for d_measure in d_measures:
            data.append(d_base + d_measure)

    pdf = pd.DataFrame(data, columns=['contract_id', 
                                     'contract_name', 
                                     'organization_marketing_name', 
                                     'parent_organization',
                                     'measure_code', 
                                     'measure_desc', 
                                     'measure_value_raw'])
    
    mimi_src_file_date = None
    mimi_src_file_date = parse(filepath.stem[-10:].replace('_','/')).date()

    assert mimi_src_file_date is not None

    pdf['measure_value'] = pdf['measure_value_raw'].apply(convert_to_num)
    pdf['performance_year'] = perf_year
    pdf['mimi_src_file_date'] = mimi_src_file_date
    pdf['mimi_src_file_name'] = filepath.name
    pdf['mimi_dlt_load_date'] = datetime.today().date()
    return pdf

# COMMAND ----------

file_lst = [filepath for filepath in Path(path).rglob("*Display_Measure*.csv")
            if filepath.stem[:4] > '2013']
file_lst += [filepath for filepath in Path(path).rglob("*HEDIS*.csv")
            if filepath.stem[:4] > '2013']
file_lst = sorted(file_lst, reverse=True)

# COMMAND ----------

pdf_lst = []
for filepath in file_lst:
    pdf_lst.append(parse_display_file(filepath))
pdf_full = pd.concat(pdf_lst)
df = spark.createDataFrame(pdf_full)

# COMMAND ----------

df.write.mode('overwrite').saveAsTable('mimi_ws_1.partcd.starrating_display_hedis_measure')

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.starrating_measure_value IS '# Part C & D Performance Data (Star Rating), raw measure values - multiyear';
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.starrating_measure_star IS '# Part C & D Performance Data (Star Rating), Stars - multiyear';
# MAGIC COMMENT ON TABLE mimi_ws_1.partcd.starrating_display_hedis_measure IS '# Part C & D Performance Data (Star Rating), Display and HEDIS values - multiyear';

# COMMAND ----------


