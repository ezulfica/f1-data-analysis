import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from botocore.config import Config


class S3Client:

    def __init__(
        self,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        region_name: str = None,
        bucket_name: str = None,
    ) -> None: 
        """
        Initialize the S3 client with AWS credentials and target bucket name.

        Args:
            aws_access_key_id (str, optional): AWS access key ID. Defaults to None.
            aws_secret_access_key (str, optional): AWS secret access key. Defaults to None.
            region_name (str, optional): AWS region name. Defaults to None.
            bucket_name (str, optional): Name of the S3 bucket to interact with. Defaults to None.
        """
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.s3_client = None
        self.bucket_name = bucket_name

    def connect(self) -> None:
        """
        Establishes a connection to AWS S3 using the provided credentials.
        
        Tries to create an S3 client and connect to the AWS S3 service. 
        If the connection fails due to missing or incorrect credentials, an error is raised.
        """
        try:
            self.s3_client = boto3.client(
                "s3",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name,
            )
            print("Connected to S3 successfully.")
        except (NoCredentialsError, PartialCredentialsError) as e:
            print(f"Error connecting to S3: {e}")
            raise

    def read_object(self, object_key: str) -> bytes | None:
        """
        Reads an object from an S3 bucket.

        Args:
            object_key (str): The key (path) of the object to be read.

        Returns:
            bytes: The content of the object as bytes.
        
        If there is an error reading the object, prints the error and returns None.
        """
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name, Key=object_key
            )
            return response["Body"].read()
        except Exception as e:
            print(
                f"Error reading object {object_key} from bucket {self.bucket_name}: {e}"
            )
            return None

    def write_object(self, object_key: str, data: bytes | str) -> None:
        """
        Uploads an object to an S3 bucket.

        Args:
            object_key (str): The key (path) where the object will be stored in S3.
            data (bytes or str): Data to be uploaded to S3. Can be in bytes or string format.
        
        If the data is a string, it is encoded as UTF-8 before uploading. 
        If an error occurs during upload, it is printed to the console.
        """
        try:
            if isinstance(data, str):
                data = data.encode("utf-8")
            self.s3_client.put_object(
                Bucket=self.bucket_name, Key=object_key, Body=data
            )
            # print(f"Successfully uploaded {object_key} to {self.bucket_name}.")
        except Exception as e:
            print(
                f"Error writing object {object_key} to bucket {self.bucket_name}: {e}"
            )

    def list_objects(self, prefix: str) -> None:
        """
        List all objects in the S3 bucket with the specified prefix using pagination.

        Args:
            prefix (str): Prefix of the object keys to list.
        
        Returns:
            list: A list of object keys in the S3 bucket that match the prefix.
        
        This method handles pagination to list large numbers of objects. 
        If there are no matching objects, it returns an empty list and prints a message.
        """
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            object_keys = []

            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                if "Contents" in page:
                    object_keys.extend(obj["Key"] for obj in page["Contents"])

            if not object_keys:
                print("Bucket is empty or no objects match the prefix.")
                return []

            return object_keys

        except Exception as e:
            print(f"Error listing objects in bucket {self.bucket_name}: {e}")
            return []

        # """
        # List all objects in an S3 bucket.
        # :param bucket_name: Name of the S3 bucket
        # :return: List of object keys
        # """
        # try:
        #     response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix = prefix)
        #     if 'Contents' in response:
        #         return [obj['Key'] for obj in response['Contents']]
        #     else:
        #         print("Bucket is empty.")
        #         return []
        # except Exception as e:
        #     print(f"Error listing objects in bucket {self.bucket_name}: {e}")
        #     return []
