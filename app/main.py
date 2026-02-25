from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from celery_app import save_in_object_storage
from s3_client import get_s3_client, create_bucket_if_not_exists
import io
import mimetypes

app = FastAPI()
BUCKET_NAME = "documents"


class EnqueuedFile(BaseModel):
    id: str
    filename: str
    status: str


@app.post("/process-document", status_code=202, response_model=EnqueuedFile)
async def process_document(file: UploadFile = File(...)):
    content = await file.read()
    s3_client = get_s3_client()

    try:
        create_bucket_if_not_exists(s3_client, BUCKET_NAME)

        file_obj = io.BytesIO(content)
        s3_client.upload_fileobj(file_obj, BUCKET_NAME, file.filename)

        task = save_in_object_storage.delay(file.filename)
        enqueued_file = EnqueuedFile(
            id=task.id,
            filename=file.filename,
            status="Processing document..."
        )

        return enqueued_file
    except Exception as exc:
        return {
            "status": "Failed to save document",
            "error": exc
        }


@app.get("/documents")
async def list_documents():
    s3_client = get_s3_client()

    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
        files = [obj['Key'] for obj in response.get('Contents', [])]

        return {"bucket": BUCKET_NAME, "files": files}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/documents/{filename}")
async def download_document(filename: str):
    s3_client = get_s3_client()

    try:
        s3_object = s3_client.get_object(Bucket=BUCKET_NAME, Key=filename)

        mime_type, _ = mimetypes.guess_type(filename)
        content_type = mime_type or "application/octet-stream"

        return StreamingResponse(
            s3_object['Body'].iter_chunks(),
            media_type=content_type,
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except s3_client.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
