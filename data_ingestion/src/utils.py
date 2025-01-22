from datetime import datetime
from aws_connect.s3_connect import S3Client
import json

def read_object_into_json(s3_client : S3Client, bucket_name : str, object_key : str) -> None: 
    obj = s3_client.read_object(bucket_name, object_key)
    return json.loads(obj)
    
def get_last_date(f1_schedule : list) : 
    date = max([schedule['date'] for schedule in f1_schedule])
    return date



