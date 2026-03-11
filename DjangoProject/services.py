import boto3
import requests
from django.conf import settings
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.MINIO_ENDPOINT,
        aws_access_key_id=settings.MINIO_ACCESS_KEY,
        aws_secret_access_key=settings.MINIO_SECRET_KEY,
    )


def upload_to_minio(file_obj, researcher_id: str, filename: str) -> str:
    """
    Upload file to MinIO under researcher's folder.
    Returns the S3 key.
    """
    s3  = get_s3_client()
    key = f"{researcher_id}/{filename}"

    s3.upload_fileobj(file_obj, settings.MINIO_BUCKET, key)
    return key


def trigger_ingest_dag(researcher_id: str, dataset: str) -> dict:
    url = f"{settings.AIRFLOW_URL}/api/v2/dags/ingest_dag/dagRuns"


    response = requests.post(
        url,
        json={"conf": {"dataset": dataset, "researcher_id": researcher_id}},
        auth=(settings.AIRFLOW_USERNAME, settings.AIRFLOW_PASSWORD),
        headers={"Content-Type": "application/json"},
        timeout=10,
        verify=False,   # ← add this
    )
    response.raise_for_status()
    return response.json()


def get_dag_run_status(dag_run_id: str) -> dict:
    url = f"{settings.AIRFLOW_URL}/api/v1/dags/ingest_dag/dagRuns/{dag_run_id}"

    response = requests.get(
        url,
        auth=(settings.AIRFLOW_USERNAME, settings.AIRFLOW_PASSWORD),
        timeout=10,
        verify=False,   # ← add this
    )
    response.raise_for_status()
    return response.json()


def list_researcher_datasets(researcher_id: str) -> list:
    """
    List all datasets for a researcher in MinIO.
    """
    s3       = get_s3_client()
    prefix   = f"{researcher_id}/"
    response = s3.list_objects_v2(Bucket=settings.MINIO_BUCKET, Prefix=prefix)
    objects  = response.get("Contents", [])

    return [
        {
            "key":           obj["Key"],
            "filename":      obj["Key"].replace(prefix, ""),
            "size_bytes":    obj["Size"],
            "last_modified": obj["LastModified"].isoformat(),
        }
        for obj in objects
        if not obj["Key"].endswith("/")
    ]


def generate_presigned_download_url(researcher_id: str, filename: str, expires: int = 3600) -> str:
    """
    Generate a presigned URL for downloading a dataset.
    """
    s3  = get_s3_client()
    key = f"{researcher_id}/{filename}"

    return s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": settings.MINIO_BUCKET, "Key": key},
        ExpiresIn=expires,
    )