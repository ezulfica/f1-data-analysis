import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

class S3Client():

    def __init__(self, aws_access_key_id: str =None, aws_secret_access_key: str =None, region_name: str =None, bucket_name : str=None):
        
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
        self.bucket_name = bucket_name

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

    def read_object(self, object_key):
        """
        Read an object from S3.
        :param bucket_name: Name of the S3 bucket
        :param object_key: Key (path) of the object
        :return: Object content as bytes
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=object_key)
            return response['Body'].read()
        except Exception as e:
            print(f"Error reading object {object_key} from bucket {self.bucket_name}: {e}")
            return None  

    def write_object(self, object_key, data):
        """
        Write an object to S3.
        :param bucket_name: Name of the S3 bucket
        :param object_key: Key (path) of the object
        :param data: Data to write (bytes or string)
        """
        try:
            if isinstance(data, str):
                data = data.encode('utf-8')
            self.s3_client.put_object(Bucket=self.bucket_name, Key=object_key, Body=data)
            print(f"Successfully uploaded {object_key} to {self.bucket_name}.")
        except Exception as e:
            print(f"Error writing object {object_key} to bucket {self.bucket_name}: {e}")

    def list_objects(self):
        """
        List all objects in an S3 bucket.
        :param bucket_name: Name of the S3 bucket
        :return: List of object keys
        """
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            else:
                print("Bucket is empty.")
                return []
        except Exception as e:
            print(f"Error listing objects in bucket {self.bucket_name}: {e}")
            return []