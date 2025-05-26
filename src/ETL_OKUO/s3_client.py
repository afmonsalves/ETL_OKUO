# src/your_project/s3_client.py
from io import StringIO
from typing import Any
import pandas as pd
import boto3
from botocore.client import BaseClient

from ETL_OKUO.config import Settings, get_settings


class S3Client:
    """
    A wrapper around boto3’s S3 client for downloading and uploading data.
    """

    def __init__(self, settings: Settings) -> None:
        """
        Initialize the S3 client with credentials from Settings.
        """
        self._client: BaseClient = boto3.client(
            "s3",
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region,
        )
        self._bucket = settings.s3_bucket

    def download_csv(self, key: str) -> pd.DataFrame:
        """
        Download a CSV file from S3 and load it directly into a pandas DataFrame.

        Args:
            key: The S3 object key (e.g. 'raw-data/data.csv').

        Returns:
            A pandas DataFrame containing the CSV data.
        """
        response = self._client.get_object(Bucket=self._bucket, Key=key)
        raw = response["Body"].read().decode("utf-8")
        return pd.read_csv(StringIO(raw))
