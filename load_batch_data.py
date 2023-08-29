import pandas as pd
from google.cloud import bigquery

project_id = 'makikuna-integral'
dataset_id = 'globant_assessment'

data_info = [
                {
                    "table_name":"hired_employees",
                    "path":"hired_employees.csv",
                    "schema":[
                            bigquery.SchemaField("id", "INTEGER"),
                            bigquery.SchemaField("name", "STRING"),
                            bigquery.SchemaField("datetime", "STRING"),
                            bigquery.SchemaField("department_id", "INTEGER"),
                            bigquery.SchemaField("job_id", "INTEGER"),
                    ]
                },
                {
                    "table_name":"departments",
                    "path":"departments.csv",
                    "schema":[
                            bigquery.SchemaField("id", "INTEGER"),
                            bigquery.SchemaField("department", "STRING")
                    ]
                },
                {
                    "table_name":"jobs",
                    "path":"jobs.csv",
                    "schema":[
                            bigquery.SchemaField("id", "INTEGER"),
                            bigquery.SchemaField("job", "STRING")
                    ]
                }
            ]

client = bigquery.Client(project=project_id)


for i in data_info:
    # Load CSV data into a Pandas DataFrame
    df = pd.read_csv(i["path"], names = [n.name for n in i["schema"]])
    # Upload the DataFrame to BigQuery
    table_ref = f"{project_id}.{dataset_id}.{i['table_name']}"
    job_config = bigquery.LoadJobConfig(
        schema=i['schema'],
        write_disposition="WRITE_TRUNCATE"  # Overwrite table if it exists
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete
    print(f"Loaded {job.output_rows} rows into {table_ref}")
