from google.cloud import bigquery
from google.cloud import storage
import json

project_id = 'makikuna-integral'
dataset_id = 'globant_assessment'
bucket_name ='globant_poc_sisalima'

with open("config_data.json") as jc:
    data_info = json.load(jc)

client = bigquery.Client(project=project_id)
client_storage = storage.Client(project=project_id)
bucket = client_storage.bucket(bucket_name)

for table in data_info:
    
    avro_file_path = f'gs://{bucket_name}/avro_output/{table["table_name"]}.avro'
    blob = bucket.blob(avro_file_path)
    blob.upload_from_filename(f'./avro_output/{table["table_name"]}.avro')
    print(f"File uploaded {avro_file_path}")

    #config the avro job bq
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.AVRO,
    )

    job = client.load_table_from_uri(
                            avro_file_path, 
                            f"{project_id}.{dataset_id}.{table['table_name']}", 
                            job_config=job_config)
    job.result()

    print(f"Table {project_id}.{dataset_id}.{table['table_name']} created and data loaded from {avro_file_path}")
