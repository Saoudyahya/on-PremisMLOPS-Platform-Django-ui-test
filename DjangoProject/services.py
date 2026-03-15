import boto3
import requests
from django.conf import settings


# ── MinIO / S3 helpers ─────────────────────────────────────────────────────────

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.MINIO_ENDPOINT,
        aws_access_key_id=settings.MINIO_ACCESS_KEY,
        aws_secret_access_key=settings.MINIO_SECRET_KEY,
    )


def upload_to_minio(file_obj, researcher_id: str, filename: str) -> str:
    """
    Upload file to MinIO at <researcher_id>/datasets/<filename>.
    Returns the S3 key.
    """
    s3  = get_s3_client()
    key = f"{researcher_id}/datasets/{filename}"
    s3.upload_fileobj(file_obj, settings.MINIO_BUCKET, key)
    return key


def list_researcher_datasets(researcher_id: str) -> list:
    """
    List all datasets stored under <researcher_id>/datasets/.
    """
    s3       = get_s3_client()
    prefix   = f"{researcher_id}/datasets/"
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
    key = f"{researcher_id}/datasets/{filename}"
    return s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": settings.MINIO_BUCKET, "Key": key},
        ExpiresIn=expires,
    )


# ── Airflow helpers ────────────────────────────────────────────────────────────

def _airflow_headers() -> dict:
    """Basic-auth headers for the Airflow REST API."""
    import base64
    credentials = base64.b64encode(
        f"{settings.AIRFLOW_USERNAME}:{settings.AIRFLOW_PASSWORD}".encode()
    ).decode()
    return {
        "Authorization": f"Basic {credentials}",
        "Content-Type":  "application/json",
    }


def trigger_notebook_launch(username: str) -> dict:
    """
    Trigger the launch_notebook_dag Airflow DAG for the given JupyterHub username.

    Calls:
        POST <AIRFLOW_URL>/api/v1/dags/launch_notebook_dag/dagRuns

    Returns the DAG run info dict on success.
    Raises RuntimeError on HTTP error.
    """
    url = f"{settings.AIRFLOW_URL}/api/v1/dags/launch_notebook_dag/dagRuns"

    payload = {
        "conf": {
            "username": username,
        }
    }

    resp = requests.post(
        url,
        json=payload,
        headers=_airflow_headers(),
        timeout=15,
    )

    if resp.status_code not in (200, 201):
        raise RuntimeError(
            f"Airflow returned {resp.status_code}: {resp.text}"
        )

    return resp.json()


def get_notebook_run_status(dag_run_id: str) -> dict:
    """
    Poll the status of a previously triggered DAG run.

    Calls:
        GET <AIRFLOW_URL>/api/v1/dags/launch_notebook_dag/dagRuns/<dag_run_id>

    Returns the DAG run info dict.
    """
    url = (
        f"{settings.AIRFLOW_URL}/api/v1/dags/launch_notebook_dag/dagRuns/{dag_run_id}"
    )

    resp = requests.get(
        url,
        headers=_airflow_headers(),
        timeout=15,
    )

    if resp.status_code != 200:
        raise RuntimeError(
            f"Airflow returned {resp.status_code}: {resp.text}"
        )

    return resp.json()