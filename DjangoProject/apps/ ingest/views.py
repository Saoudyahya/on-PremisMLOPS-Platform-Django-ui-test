from rest_framework.views import APIView
from rest_framework.parsers import MultiPartParser
from rest_framework.response import Response
from rest_framework import status
from mlops_api.services import (
    upload_to_minio,
    trigger_ingest_dag,
    get_dag_run_status,
    list_researcher_datasets,
    generate_presigned_download_url,
)


class IngestDatasetView(APIView):
    """
    POST /api/ingest/
    Multipart form: file + researcher_id

    1. Uploads file to MinIO s3://mlops-dvc/<researcher_id>/<filename>
    2. Triggers Airflow ingest_dag
    3. Returns dag_run_id for status polling
    """
    parser_classes = [MultiPartParser]

    def post(self, request):
        file = request.FILES.get("file")
        researcher_id = request.data.get("researcher_id")

        # Validate
        if not file:
            return Response({"error": "file is required"}, status=status.HTTP_400_BAD_REQUEST)
        if not researcher_id:
            return Response({"error": "researcher_id is required"}, status=status.HTTP_400_BAD_REQUEST)

        filename = file.name

        try:
            # Step 1 — upload to MinIO
            s3_key = upload_to_minio(file, researcher_id, filename)

            # Step 2 — trigger DAG
            dag_run = trigger_ingest_dag(researcher_id, filename)

        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_502_BAD_GATEWAY)

        return Response({
            "message": "Dataset uploaded and ingestion started",
            "researcher_id": researcher_id,
            "dataset": filename,
            "s3_key": s3_key,
            "dag_run_id": dag_run["dag_run_id"],
            "dag_state": dag_run["state"],
        }, status=status.HTTP_202_ACCEPTED)


class IngestStatusView(APIView):
    """
    GET /api/ingest/status/<dag_run_id>/
    Poll the status of an ingestion DAG run.
    """

    def get(self, request, dag_run_id):
        try:
            run = get_dag_run_status(dag_run_id)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_502_BAD_GATEWAY)

        return Response({
            "dag_run_id": dag_run_id,
            "state": run["state"],  # queued | running | success | failed
            "start_date": run.get("start_date"),
            "end_date": run.get("end_date"),
        })


class DatasetListView(APIView):
    """
    GET /api/datasets/<researcher_id>/
    List all datasets for a researcher in MinIO.
    """

    def get(self, request, researcher_id):
        try:
            datasets = list_researcher_datasets(researcher_id)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_502_BAD_GATEWAY)

        return Response({
            "researcher_id": researcher_id,
            "count": len(datasets),
            "datasets": datasets,
        })


class DatasetDownloadView(APIView):
    """
    GET /api/datasets/<researcher_id>/<filename>/download/
    Returns a presigned MinIO URL for direct download.
    """

    def get(self, request, researcher_id, filename):
        try:
            url = generate_presigned_download_url(researcher_id, filename)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_502_BAD_GATEWAY)

        return Response({
            "url": url,
            "expires": "3600s",
        })