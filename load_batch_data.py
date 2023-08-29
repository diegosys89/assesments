import pandas as pd
from google.cloud import bigquery
import json

project_id = 'makikuna-integral'
dataset_id = 'globant_assessment'

with open("config_data.json") as jc:
    data_info = json.load(jc)

client = bigquery.Client(project=project_id)

for i in data_info:
    # Load CSV data into a Pandas DataFrame
    df = pd.read_csv(i["path"], names = [n["name"] for n in i["schema"]])
    # Upload the DataFrame to BigQuery
    table_ref = f"{project_id}.{dataset_id}.{i['table_name']}"
    job_config = bigquery.LoadJobConfig(
        schema=[bigquery.SchemaField(data_schema["name"],data_schema["type"]) for data_schema in i["schema"]],
        write_disposition="WRITE_TRUNCATE"  # Overwrite table if it exists
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete
    print(f"Loaded {job.output_rows} rows into {table_ref}")
