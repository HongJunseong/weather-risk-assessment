from contextlib import redirect_stdout
import io
import os
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest.mock import patch

import boto3
from botocore.config import Config
import pandas as pd

from tests.test_bronze_contract import valid_frames
from weather_risk_assessment.contracts.bronze import CONTRACTS
from weather_risk_assessment.scripts.upload_bronze_to_s3 import main as upload_bronze


ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "").strip()
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin123"
RUN_DT = "2025010208"


@unittest.skipUnless(ENDPOINT_URL, "S3_ENDPOINT_URL is required for MinIO tests")
class MinioIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bucket = f"weather-risk-test-{uuid.uuid4().hex[:12]}"
        cls.client = boto3.client(
            "s3",
            endpoint_url=ENDPOINT_URL,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            region_name="ap-northeast-2",
            config=Config(s3={"addressing_style": "path"}),
        )
        cls.client.create_bucket(Bucket=cls.bucket)

    @classmethod
    def tearDownClass(cls):
        response = cls.client.list_objects_v2(Bucket=cls.bucket)
        for item in response.get("Contents", []):
            cls.client.delete_object(Bucket=cls.bucket, Key=item["Key"])
        cls.client.delete_bucket(Bucket=cls.bucket)

    def test_uploads_validated_bronze_partition_to_s3_api(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / f"dt={RUN_DT}"
            run_dir.mkdir()
            for dataset, frame in valid_frames().items():
                frame.to_parquet(run_dir / CONTRACTS[dataset].filename, index=False)

            environment = {
                "AWS_ACCESS_KEY_ID": ACCESS_KEY,
                "AWS_SECRET_ACCESS_KEY": SECRET_KEY,
                "AWS_REGION": "ap-northeast-2",
                "S3_RISK_STREAM_BUCKET": self.bucket,
                "S3_ENDPOINT_URL": ENDPOINT_URL,
            }
            with patch.dict(os.environ, environment), redirect_stdout(io.StringIO()):
                upload_bronze(run_dt=RUN_DT, sink_dir=run_dir)

        response = self.client.list_objects_v2(Bucket=self.bucket)
        keys = {item["Key"] for item in response.get("Contents", [])}
        expected = {
            f"bronze/kma/{dataset}/dt={RUN_DT}/{contract.filename}"
            for dataset, contract in CONTRACTS.items()
        }
        self.assertEqual(keys, expected)

        short_key = f"bronze/kma/short_fcst/dt={RUN_DT}/short_fcst.parquet"
        body = self.client.get_object(Bucket=self.bucket, Key=short_key)["Body"].read()
        self.assertEqual(len(pd.read_parquet(io.BytesIO(body))), 1)
