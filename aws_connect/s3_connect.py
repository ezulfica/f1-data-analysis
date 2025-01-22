import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import json
class S3Client():

    def __init__(self, aws_access_key_id: str =None, aws_secret_access_key: str =None, region_name: str =None):
        """
        Initialize the S3 client.
        :param aws_access_key_id: AWS access key ID
        :param aws_secret_access_key: AWS secret access key
        :param region_name: AWS region name
        """
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.s3_client = None

    def connect(self):
        """
        Establishes a connection to the S3 service.
        """
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name
            )
            print("Connected to S3 successfully.")
        except (NoCredentialsError, PartialCredentialsError) as e:
            print(f"Error connecting to S3: {e}")
            raise

    def read_object(self, bucket_name, object_key):
        """
        Read an object from S3.
        :param bucket_name: Name of the S3 bucket
        :param object_key: Key (path) of the object
        :return: Object content as bytes
        """
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
            return response['Body'].read()
        except Exception as e:
            print(f"Error reading object {object_key} from bucket {bucket_name}: {e}")
            return None  

    def write_object(self, bucket_name, object_key, data):
        """
        Write an object to S3.
        :param bucket_name: Name of the S3 bucket
        :param object_key: Key (path) of the object
        :param data: Data to write (bytes or string)
        """
        try:
            if isinstance(data, str):
                data = data.encode('utf-8')
            self.s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=data)
            print(f"Successfully uploaded {object_key} to {bucket_name}.")
        except Exception as e:
            print(f"Error writing object {object_key} to bucket {bucket_name}: {e}")