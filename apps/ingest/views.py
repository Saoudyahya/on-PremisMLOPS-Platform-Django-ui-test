from rest_framework.views import APIView
from rest_framework.parsers import MultiPartParser
from rest_framework.response import Response
from rest_framework import status
from DjangoProject.services import (
    upload_to_minio,
    list_researcher_datasets,
    generate_presigned_download_url,
)


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