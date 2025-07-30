import boto3
import os
def upload_to_s3(**kwargs):
    client = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION")
    )
    bucket_name = os.getenv("BUCKET_NAME")
    print("Uploading to bucket:", bucket_name)
    local_folder = kwargs['local_folder']
    for root, _, files in os.walk(local_folder):
        for file in files:
            if file.endswith('.parquet'):
                local_path = os.path.join(root, file)
                s3_key = os.path.relpath(local_path, local_folder).replace("\\", "/")
                client.upload_file(local_path, bucket_name, s3_key)
                print(f"Uploaded: {local_path} -> s3://{bucket_name}/{s3_key}")





