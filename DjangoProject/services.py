import boto3
import requests
from datetime import datetime, timezone
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
    s3  = get_s3_client()
    key = f"{researcher_id}/datasets/{filename}"
    s3.upload_fileobj(file_obj, settings.MINIO_BUCKET, key)
    return key


def list_researcher_datasets(researcher_id: str) -> list:
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
    s3  = get_s3_client()
    key = f"{researcher_id}/datasets/{filename}"
    return s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": settings.MINIO_BUCKET, "Key": key},
        ExpiresIn=expires,
    )


# ── Airflow helpers ────────────────────────────────────────────────────────────

def _get_airflow_jwt() -> str:
    """
    Airflow 3 requires a JWT obtained via POST /auth/token.
    Basic Auth directly on /api/v2/ is not supported.
    """
    resp = requests.post(
        f"{settings.AIRFLOW_URL}/auth/token",
        json={
            "username": settings.AIRFLOW_USERNAME,
            "password": settings.AIRFLOW_PASSWORD,
        },
        headers={"Content-Type": "application/json"},
        timeout=15,
    )
    if resp.status_code not in (200, 201):
        raise RuntimeError(
            f"Airflow /auth/token returned {resp.status_code}: {resp.text}"
        )
    data  = resp.json()
    token = data.get("access_token") or data.get("token")
    if not token:
        raise RuntimeError(f"Airflow /auth/token had no token field: {resp.text}")
    return token


def _airflow_headers() -> dict:
    return {
        "Authorization": f"Bearer {_get_airflow_jwt()}",
        "Content-Type":  "application/json",
    }


def _ensure_dag_unpaused(dag_id: str) -> None:
    """
    PATCH /api/v2/dags/<dag_id> with is_paused=false.
    Airflow ignores this if the DAG is already unpaused.
    is_paused_upon_creation=False in the DAG file only works for brand-new DAGs
    that have never been registered — once a DAG exists in the DB as paused,
    that flag has no effect. This call fixes it unconditionally.
    """
    resp = requests.patch(
        f"{settings.AIRFLOW_URL}/api/v2/dags/{dag_id}",
        json={"is_paused": False},
        headers=_airflow_headers(),
        timeout=15,
    )
    if resp.status_code not in (200, 201):
        raise RuntimeError(
            f"Failed to unpause DAG {dag_id}: {resp.status_code}: {resp.text}"
        )


def trigger_dag(dag_id: str, conf: dict) -> dict:
    """
    Generic helper: unpause a DAG then trigger a run with the given conf.
    Returns the DAG run info dict.
    """
    _ensure_dag_unpaused(dag_id)

    resp = requests.post(
        f"{settings.AIRFLOW_URL}/api/v2/dags/{dag_id}/dagRuns",
        json={
            "conf":         conf,
            # Airflow 3 requires logical_date in the POST body
            "logical_date": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
        headers=_airflow_headers(),
        timeout=15,
    )
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Airflow returned {resp.status_code}: {resp.text}")
    return resp.json()


def trigger_notebook_launch(username: str) -> dict:
    return trigger_dag("launch_notebook_dag", {"username": username})


def trigger_ingest(dataset: str, researcher_id: str) -> dict:
    return trigger_dag("ingest_dag", {"dataset": dataset, "researcher_id": researcher_id})


def get_dag_run_status(dag_id: str, dag_run_id: str) -> dict:
    resp = requests.get(
        f"{settings.AIRFLOW_URL}/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}",
        headers=_airflow_headers(),
        timeout=15,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Airflow returned {resp.status_code}: {resp.text}")
    return resp.json()


def get_notebook_run_status(dag_run_id: str) -> dict:
    return get_dag_run_status("launch_notebook_dag", dag_run_id)