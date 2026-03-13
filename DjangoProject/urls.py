from django.urls import path
from apps.ingest.views import (
    IngestDatasetView,
    DatasetListView,
    DatasetDownloadView,
)

urlpatterns = [
    path("api/ingest/",                                               IngestDatasetView.as_view()),
    path("api/datasets/<str:researcher_id>/",                         DatasetListView.as_view()),
    path("api/datasets/<str:researcher_id>/<str:filename>/download/", DatasetDownloadView.as_view()),
]