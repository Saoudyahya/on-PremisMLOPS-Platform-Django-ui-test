from decouple import config

SECRET_KEY = config("DJANGO_SECRET_KEY", default="change-me-in-production")
DEBUG      = config("DEBUG", default=True, cast=bool)
ALLOWED_HOSTS = config("ALLOWED_HOSTS", default="*").split(",")

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "rest_framework",
    "corsheaders",
    "apps.ingest",
]

MIDDLEWARE = [
    "corsheaders.middleware.CorsMiddleware",
    "django.middleware.common.CommonMiddleware",
]

ROOT_URLCONF = "DjangoProject.urls"
CORS_ALLOW_ALL_ORIGINS = True

# Prevents Django from redirecting POST /api/notebook/launch → 301 (which drops the body)
APPEND_SLASH = False

# MinIO
MINIO_ENDPOINT   = config("MINIO_ENDPOINT",   default="http://minio-svc.mlops-minio.svc.cluster.local:9000")
MINIO_ACCESS_KEY = config("MINIO_ACCESS_KEY", default="minio-admin")
MINIO_SECRET_KEY = config("MINIO_SECRET_KEY", default="minio-admin")
MINIO_BUCKET     = config("MINIO_BUCKET",     default="mlops-dvc")

# Airflow
AIRFLOW_URL      = config("AIRFLOW_URL",      default="http://airflow-api-server.mlops-airflow.svc.cluster.local:8080")
AIRFLOW_USERNAME = config("AIRFLOW_USERNAME", default="admin")
AIRFLOW_PASSWORD = config("AIRFLOW_PASSWORD", default="admin123")

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"