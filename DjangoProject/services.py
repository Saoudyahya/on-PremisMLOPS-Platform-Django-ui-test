import boto3
from django.conf import settings


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