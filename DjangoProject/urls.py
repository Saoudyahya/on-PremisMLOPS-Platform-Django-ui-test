from django.urls import path
from apps import (
    IngestDatasetView,
    IngestStatusView,
    DatasetListView,
    DatasetDownloadView,
)

urlpatterns = [
    path("api/ingest/",                                               IngestDatasetView.as_view()),
    path("api/ingest/status/<str:dag_run_id>/",                       IngestStatusView.as_view()),
    path("api/datasets/<str:researcher_id>/",                         DatasetListView.as_view()),
    path("api/datasets/<str:researcher_id>/<str:filename>/download/", DatasetDownloadView.as_view()),
]