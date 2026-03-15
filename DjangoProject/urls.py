from django.urls import path
from apps.ingest.views import (
    IngestDatasetView,
    DatasetListView,
    DatasetDownloadView,
    LaunchNotebookView,
    NotebookStatusView,
)

urlpatterns = [
    # ── Dataset endpoints ──────────────────────────────────────────────────────
    path("api/ingest",
         IngestDatasetView.as_view()),

    path("api/datasets/<str:researcher_id>",
         DatasetListView.as_view()),

    path("api/datasets/<str:researcher_id>/<str:filename>/download",
         DatasetDownloadView.as_view()),

    # ── Notebook endpoints ─────────────────────────────────────────────────────
    # POST  {"username": "researcher_abc"}  → triggers launch_notebook_dag
    path("api/notebook/launch",
         LaunchNotebookView.as_view()),

    # GET   → poll DAG run state (queued / running / success / failed)
    path("api/notebook/status/<str:dag_run_id>",
         NotebookStatusView.as_view()),
]