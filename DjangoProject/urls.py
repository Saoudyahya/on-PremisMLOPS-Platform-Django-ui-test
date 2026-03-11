from django.urls import path
from mlops_api.apps.ingest.views import (
    IngestDatasetView,
    IngestStatusView,
    DatasetListView,
    DatasetDownloadView,
)

urlpatterns = [
    # Ingest
    path("api/ingest/",                                          IngestDatasetView.as_view()),
    path("api/ingest/status/<str:dag_run_id>/",                  IngestStatusView.as_view()),

    # Datasets
    path("api/datasets/<str:researcher_id>/",                    DatasetListView.as_view()),
    path("api/datasets/<str:researcher_id>/<str:filename>/download/", DatasetDownloadView.as_view()),
]