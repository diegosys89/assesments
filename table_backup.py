from google.cloud import bigquery 
from google.cloud import storage
import json

client = bigquery.Client()
project_id = 'makikuna-integral'
dataset_id = 'globant_assessment'
bucket_name ='globant_poc_sisalima'

with open("config_data.json") as jc:
    data_info = json.load(jc)

for table in data_info:
    destination_uri = f'gs://{bucket_name}/avro_output/{table["table_name"]}.avro'
    dataset_ref = client.dataset(dataset_id, project=project_id)
    table_ref = dataset_ref.table(table["table_name"])

    job_config = bigquery.job.ExtractJobConfig()
    job_config.destination_format = bigquery.DestinationFormat.AVRO

    extract_job = client.extract_table(
            table_ref,
            destination_uri,
            job_config=job_config,
            location="US",
            )  
    extract_job.result()

print("Extract success")
print("Downloading to local path")

prefix = 'avro_output/' 
client = storage.Client(project=project_id)
bucket = client.bucket(bucket_name)
blobs = bucket.list_blobs(prefix=prefix)

for blob in blobs:
    local_file_path = f'./avro_output/{blob.name[len(prefix):]}'
    blob.download_to_filename(local_file_path)
    print(f"File downloaded: {local_file_path}")

print("All files downloaded.")

