from rest_framework.views import APIView
from rest_framework.parsers import MultiPartParser
from rest_framework.response import Response
from rest_framework import status
from DjangoProject.services import (
    upload_to_minio,
    list_researcher_datasets,
    generate_presigned_download_url,
    trigger_notebook_launch,
    get_notebook_run_status,
)


# ── Dataset endpoints ──────────────────────────────────────────────────────────

class IngestDatasetView(APIView):
    """
    POST /api/ingest/
    Multipart form: file + researcher_id

    Uploads the file to MinIO at:
        s3://mlops-dvc/<researcher_id>/datasets/<filename>
    """
    parser_classes = [MultiPartParser]

    def post(self, request):
        file          = request.FILES.get("file")
        researcher_id = request.data.get("researcher_id")

        if not file:
            return Response({"error": "file is required"}, status=status.HTTP_400_BAD_REQUEST)
        if not researcher_id:
            return Response({"error": "researcher_id is required"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            s3_key = upload_to_minio(file, researcher_id, file.name)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_502_BAD_GATEWAY)

        return Response({
            "message":       "Dataset uploaded successfully",
            "researcher_id": researcher_id,
            "filename":      file.name,
            "s3_key":        s3_key,
        }, status=status.HTTP_201_CREATED)


class DatasetListView(APIView):
    """
    GET /api/datasets/<researcher_id>/
    List all datasets stored for a researcher.
    """

    def get(self, request, researcher_id):
        try:
            datasets = list_researcher_datasets(researcher_id)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_502_BAD_GATEWAY)

        return Response({
            "researcher_id": researcher_id,
            "count":         len(datasets),
            "datasets":      datasets,
        })


class DatasetDownloadView(APIView):
    """
    GET /api/datasets/<researcher_id>/<filename>/download/
    Returns a presigned MinIO URL valid for 1 hour.
    """

    def get(self, request, researcher_id, filename):
        try:
            url = generate_presigned_download_url(researcher_id, filename)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_502_BAD_GATEWAY)

        return Response({
            "url":     url,
            "expires": "3600s",
        })


# ── Notebook endpoints ─────────────────────────────────────────────────────────

class LaunchNotebookView(APIView):
    """
    POST /api/notebook/launch/
    JSON body: { "username": "<jupyterhub_username>" }

    Triggers the launch_notebook_dag Airflow DAG which:
      1. Creates the JupyterHub user if it doesn't exist
      2. Spawns the notebook server
      3. Polls until the server is ready

    Returns the Airflow DAG run ID so the client can poll status separately.

    Example request:
        curl -X POST http://localhost:8000/api/notebook/launch/ \\
             -H "Content-Type: application/json" \\
             -d '{"username": "researcher_abc"}'

    Example response:
        {
            "message":     "Notebook launch triggered",
            "username":    "researcher_abc",
            "dag_run_id":  "manual__2026-03-15T00:27:19.203409+00:00",
            "state":       "queued",
            "notebook_url": "http://proxy-public.mlops-jupyterhub.svc.cluster.local:80/user/researcher_abc/lab"
        }
    """

    def post(self, request):
        username = request.data.get("username", "").strip()

        if not username:
            return Response(
                {"error": "username is required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            dag_run = trigger_notebook_launch(username)
        except RuntimeError as e:
            return Response({"error": str(e)}, status=status.HTTP_502_BAD_GATEWAY)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        dag_run_id = dag_run.get("dag_run_id", "")

        return Response(
            {
                "message":      "Notebook launch triggered",
                "username":     username,
                "dag_run_id":   dag_run_id,
                "state":        dag_run.get("state", "queued"),
                # Convenience link — the server won't be ready immediately.
                # Poll /api/notebook/status/<dag_run_id>/ until state == "success".
                "notebook_url": f"http://jupyter.yourdomain.com/user/{username}/lab",
            },
            status=status.HTTP_202_ACCEPTED,
        )


class NotebookStatusView(APIView):
    """
    GET /api/notebook/status/<dag_run_id>/
    Poll the status of a notebook launch DAG run.

    Possible states returned by Airflow:
        queued | running | success | failed

    When state == "success" the notebook server is ready and notebook_url is live.

    Example request:
        curl http://localhost:8000/api/notebook/status/manual__2026-03-15T00:27:19.203409+00:00/

    Example response:
        {
            "dag_run_id": "manual__2026-03-15T00:27:19.203409+00:00",
            "state":      "success",
            "ready":      true
        }
    """

    def get(self, request, dag_run_id):
        try:
            run_info = get_notebook_run_status(dag_run_id)
        except RuntimeError as e:
            return Response({"error": str(e)}, status=status.HTTP_502_BAD_GATEWAY)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        state = run_info.get("state", "unknown")

        return Response({
            "dag_run_id": dag_run_id,
            "state":      state,
            "ready":      state == "success",
        })