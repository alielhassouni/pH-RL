from util import logging as log
import io
import boto3
from botocore.client import ClientError
from configs import s3
import pandas as pd
from io import StringIO # python3; python2: BytesIO
from datetime import datetime
import pandas as pd


class S3Client(object):

    def __init__(self, bucket_location="eu-central-1"):
        self.s3_session = boto3.session.Session(region_name="eu-central-1")
        self.s3 = self.s3_session.resource("s3")
        self.s3_client = boto3.client(service_name="s3", endpoint_url="http://s3-eu-central-1.amazonaws.com")
        self.logger = log.get_logger("S3 client")
        pass

    def bucket_exists(self, bucket_name: str):
        self.logger.debug("Testing bucket access")
        try:
            bucket = self.s3.Bucket(bucket_name)
            self.s3.meta.client.head_bucket(Bucket=bucket.name)
            self.logger.debug("Bucket: {} - Accessible.".format(bucket_name))
            return True
        except ClientError as e:
            error_code = int(e.response['Error']['Code'])
            error_message = str(e.response['Error']['Message'])
            if error_code == 403:
                self.logger.warning("Private Bucket: {} - {}.".format(bucket_name, error_message))
                return True
            elif error_code == 404:
                self.logger.warning("Bucket: {} - {}.".format(bucket_name, error_message))
                return False

    def list(self, bucket_name: str, prefix=""):
        bucket = self.s3.Bucket(bucket_name)
        for s3object in bucket.objects.filter(Prefix=prefix):
            yield s3object.key

    def get_object(self, bucket_name: str, object_name: str):
        bucket = self.s3.Bucket(bucket_name)
        return bucket.Object(object_name)

    def get_string(self, object):
        with self.get_bytes(object) as data:
            return data.getvalue().decode("utf-8")

    def get_bytes(self, object):
        data = io.BytesIO()
        object.download_fileobj(data)
        data.flush()
        return data

    def write_object(self, file, bucket_name: str, key, prefix):
        bucket = bucket_name
        csv_buffer = StringIO()
        file.to_csv(csv_buffer)
        s3_resource = boto3.resource('s3')
        s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())


def main():
    client = S3Client("eu-central-1")
    column_names = ["timestamp", "day_part", "user_id", "action", "message"]
    df = pd.DataFrame(columns=column_names)
    data = pd.DataFrame({"timestamp": datetime.now(tz=None), "day_part": 1, "user_id": "GO9Y0W4S", "action": 1, "message": "test entry"}, index=[0])
    df = df.append(data)
    client.write_object(file=df, bucket_name="your_s3_bucket_name", key="test/actions/actions.csv", prefix="/test/actions")


if __name__ == "__main__":
    main()

