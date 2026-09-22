from contextlib import redirect_stdout
import io
import os
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest.mock import patch

import boto3
import pandas as pd

from tests.test_bronze_contract import valid_frames
from weather_risk_assessment.contracts.bronze import CONTRACTS
from weather_risk_assessment.scripts.upload_bronze_to_s3 import main as upload_bronze


BUCKET = os.getenv("AWS_INTEGRATION_BUCKET", "").strip()
RUN_DT = "2025010208"


@unittest.skipUnless(BUCKET, "AWS_INTEGRATION_BUCKET is required for AWS S3 tests")
class AwsS3IntegrationTests(unittest.TestCase):
    def test_uploads_and_reads_bronze_partition(self):
        prefix = f"integration-tests/{uuid.uuid4().hex}/bronze/kma"
        client = boto3.client("s3")
        try:
            with tempfile.TemporaryDirectory() as tmp:
                run_dir = Path(tmp) / f"dt={RUN_DT}"
                run_dir.mkdir()
                for dataset, frame in valid_frames().items():
                    frame.to_parquet(
                        run_dir / CONTRACTS[dataset].filename, index=False
                    )

                environment = {
                    "BRONZE_PREFIX": prefix,
                    "S3_ENDPOINT_URL": "",
                    "S3_RISK_STREAM_BUCKET": BUCKET,
                }
                with patch.dict(os.environ, environment), redirect_stdout(io.StringIO()):
                    upload_bronze(run_dt=RUN_DT, sink_dir=run_dir)

            response = client.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
            keys = {item["Key"] for item in response.get("Contents", [])}
            expected = {
                f"{prefix}/{dataset}/dt={RUN_DT}/{contract.filename}"
                for dataset, contract in CONTRACTS.items()
            }
            self.assertEqual(keys, expected)

            short_key = f"{prefix}/short_fcst/dt={RUN_DT}/short_fcst.parquet"
            body = client.get_object(Bucket=BUCKET, Key=short_key)["Body"].read()
            self.assertEqual(len(pd.read_parquet(io.BytesIO(body))), 1)
        finally:
            response = client.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
            objects = [{"Key": item["Key"]} for item in response.get("Contents", [])]
            if objects:
                client.delete_objects(Bucket=BUCKET, Delete={"Objects": objects})


if __name__ == "__main__":
    unittest.main()
