import json
import os
from typing import Dict, Set, List

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud import storage


PROJECT_ID = os.environ["PROJECT_ID"]
GCP_REGION = os.environ["GCP_REGION"]

RAW_BUCKET = os.environ["RAW_BUCKET"]  # checkpoint ide ovdje
PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]
PREFIX = os.environ.get("PREFIX", "topic")

BQ_DATASET = os.environ["BQ_DATASET"]
BQ_TABLE = os.environ["BQ_TABLE"]

CHECKPOINT_OBJECT = os.getenv(
    "CHECKPOINT_OBJECT",
    f"{PREFIX}/_checkpoints/bq_loader_state.json",
)


def ensure_dataset(client: bigquery.Client, dataset_id: str) -> None:
    try:
        client.get_dataset(dataset_id)
        print(f"Dataset exists: {dataset_id}")
    except NotFound:
        ds = bigquery.Dataset(dataset_id)
        ds.location = GCP_REGION
        client.create_dataset(ds)
        print(f"Created dataset: {dataset_id} (location={GCP_REGION})")


def read_checkpoint(gcs: storage.Client) -> Dict:
    bucket = gcs.bucket(RAW_BUCKET)
    blob = bucket.blob(CHECKPOINT_OBJECT)

    if not blob.exists():
        return {"loaded_hour_folders": []}

    data = blob.download_as_bytes()
    obj = json.loads(data.decode("utf-8"))

    # backward-compat: ako ima stari last_loaded_ts, ignoriraj ga
    if "loaded_hour_folders" not in obj:
        obj["loaded_hour_folders"] = []

    return obj


def write_checkpoint(gcs: storage.Client, checkpoint: Dict) -> None:
    bucket = gcs.bucket(RAW_BUCKET)
    blob = bucket.blob(CHECKPOINT_OBJECT)
    blob.upload_from_string(
        json.dumps(checkpoint, ensure_ascii=False, indent=2),
        content_type="application/json",
    )


def extract_hour_folder(object_name: str) -> str:
    """
    object_name primjer:
    stackoverflow/year=2025/month=12/day=16/hour=15/processed/part-xxx.parquet

    vraća:
    stackoverflow/year=2025/month=12/day=16/hour=15/processed
    """
    parts = object_name.split("/")
    return "/".join(parts[:-1])


def list_all_hour_folders(gcs: storage.Client) -> Set[str]:
    """
    Vrati set svih hour-folders koji imaju bar jedan parquet:
    stackoverflow/year=.../month=.../day=.../hour=.../processed
    """
    bucket = gcs.bucket(PROCESSED_BUCKET)
    list_prefix = f"{PREFIX}/"

    hour_folders: Set[str] = set()

    for blob in gcs.list_blobs(bucket, prefix=list_prefix):
        name = blob.name
        if "/processed/" not in name or not name.endswith(".parquet"):
            continue
        hour_folders.add(extract_hour_folder(name))

    return hour_folders


def load_hour_folder(bq: bigquery.Client, table_id: str, hour_folder: str) -> None:
    gcs_uri = f"gs://{PROCESSED_BUCKET}/{hour_folder}/*.parquet"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )

    print(f"Loading: {gcs_uri} -> {table_id}")
    job = bq.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    job.result()
    print(f"Loaded. Job ID: {job.job_id}")


def main():
    gcs = storage.Client()
    bq = bigquery.Client(project=PROJECT_ID, location=GCP_REGION)

    dataset_id = f"{PROJECT_ID}.{BQ_DATASET}"
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    ensure_dataset(bq, dataset_id)

    checkpoint = read_checkpoint(gcs)
    loaded: Set[str] = set(checkpoint.get("loaded_hour_folders", []))

    print(f"Checkpoint loaded_hour_folders = {len(loaded)}")

    all_hour_folders = list_all_hour_folders(gcs)
    to_load = sorted(all_hour_folders - loaded)

    if not to_load:
        print("No new hour folders found. Nothing to load.")
        return

    loaded_now: List[str] = []
    for hour_folder in to_load:
        load_hour_folder(bq, table_id, hour_folder)
        loaded.add(hour_folder)
        loaded_now.append(hour_folder)

    checkpoint["loaded_hour_folders"] = sorted(list(loaded))
    write_checkpoint(gcs, checkpoint)

    print(f"Loaded {len(loaded_now)} new hour folders.")
    print(f"Updated checkpoint: gs://{RAW_BUCKET}/{CHECKPOINT_OBJECT}")


if __name__ == "__main__":
    main()
