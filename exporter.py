import logging
import os
import time
from pathlib import Path

import boto3
import duckdb
import pyarrow.parquet as pq
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
load_dotenv()

from queries import QUERIES

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename="pipeline.log",
    encoding="utf-8",
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)


class NBAQueryExporter:
    """
    Runs the 10 analytical SQL queries against the DuckDB star schema,
    exports each result set to Parquet, and uploads to S3.
    """

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        output_dir,
        s3_bucket,
        s3_prefix = "exports",
        ):
        self.conn = conn
        self.output_dir = Path(output_dir)
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix.rstrip("/")

        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION", "us-east-1"),
        )
        logger.info("NBAQueryExporter initialized")

    def _run_query(self, name, sql):
        """Execute a single query and write results to a local Parquet file."""
        logger.info(f"Running query: {name}")
        try:
            arrow_table = self.conn.execute(sql).fetch_arrow_table()
            local_path = self.output_dir / f"{name}.parquet"
            pq.write_table(arrow_table, local_path)
            logger.info(
                f"Query {name} — {arrow_table.num_rows} rows written to {local_path}")
            return local_path
        except Exception as e:
            logger.error(f"Query {name} failed: {e}")
            raise

    def _upload_to_s3(self, local_path, theme):
        """Upload a local Parquet file to S3 and return the S3 URI."""
        s3_key = f"{self.s3_prefix}/{theme}/{local_path.name}"
        s3_uri = f"s3://{self.s3_bucket}/{s3_key}"
        logger.info(f"Uploading {local_path.name} to {s3_uri}")
        try:
            self.s3_client.upload_file(str(local_path), self.s3_bucket, s3_key)
            logger.info(f"Upload complete: {s3_uri}")
            return s3_uri
        except (BotoCoreError, ClientError) as e:
            logger.error(f"S3 upload failed for {local_path.name}: {e}")
            raise

    def run_all(self):
        """
        Run all 10 queries, export to Parquet, upload to S3.
        Returns a dict mapping query name → S3 URI.
        Logs duration of each phase.
        """
        logger.info("NBAQueryExporter starting export run")

        for name, (theme, sql) in QUERIES.items():
            try:
                local_path = self._run_query(name, sql)
                self._upload_to_s3(local_path, theme)
            except Exception as e:
                logger.error(f"Failed to upload file to AWS S3 bucket: {e}")
                raise
        
        logger.info("NBAQueryExporter files exported and uploaded to S3")


def main():

    DATA_DIR = os.getenv("DATA_DIR")
    S3_BUCKET = os.getenv("S3_BUCKET")
    S3_PREFIX = os.getenv("S3_PREFIX", "nba-analytics/exports")

    # Re-open the connection to the existing DuckDB file
    db_file = str(Path(DATA_DIR) / "db" / "nba_analytics.duckdb")
    conn = duckdb.connect(db_file)

    exporter = NBAQueryExporter(
        conn=conn,
        output_dir=str(Path(DATA_DIR) / "parquet_exports"),
        s3_bucket=S3_BUCKET,
        s3_prefix=S3_PREFIX,
    )
    exporter.run_all()

if __name__ == "__main__":
    main()