import pandas as pd
import boto3

bucket = "air-traffic-data"

s3 = boto3.resource("s3")
s3_bucket = s3.Bucket(bucket)
files_in_s3 = [f.key for f in s3_bucket.objects.filter().all() if f.key.endswith('.csv')]

# print(files_in_s3)

# obj = s3.get_object(Bucket= bucket, Key= file_name) 
# get object and file (key) from bucket

def convert_to_df(file):
    obj = s3.get_object(Bucket=bucket, Key=file)
    df = pd.read_csv(obj['Body']) # 'Body' is a key word


for file in files_in_s3:
    convert_to_df(file)