import boto3
from botocore.client import BaseClient
from config import Settings, get_settings
from io import StringIO, BytesIO
import pandas as pd


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
            aws_access_key_id='AKIA33UN72HYMQL7J7JP',
            aws_secret_access_key='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',
            region_name='us-west-2',
        )
        self._bucket = 'assessment-86fc5eb8'
        self._input_path = 'raw-data/data.csv'
        self._output_path = 'cleaned-data/AMS569/data.parquet'

    def download_csv(self, key: str) -> pd.DataFrame:
        """
        Download a CSV file from S3 and load it directly into a pandas DataFrame.

        Args:
            key: The S3 object key (stored in .env).

        Returns:
            A pandas DataFrame containing the CSV data.
        """
        response = self._client.get_object(Bucket=self._bucket, Key=key)
        raw = response["Body"].read().decode("utf-8")
        return pd.read_csv(StringIO(raw))

    def upload_parquet(self, df: pd.DataFrame, key: str) -> None:
        """
        Upload the clean dataFrame to S3 in Parquet format.

        Args:
            df: The DataFrame to upload.
            key: The destination S3 key (stored in .env).
        """
        buffer = BytesIO()
        # Write DataFrame to the in-memory buffer in Parquet format
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)
        self._client.put_object(Bucket=self._bucket, Key=key, Body=buffer.getvalue())
