from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from elasticsearch import Elasticsearch
from typing import List, Optional
from app.celery_app import apply_ocr
from app.s3_client import get_s3_client, create_bucket_if_not_exists
import mimetypes
import io
from pathlib import Path
from pdf2image import convert_from_path
import pytesseract

app = FastAPI()

UPLOAD_DIR = Path("uploaded documents")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

es = Elasticsearch(
    ["http://elasticsearch:9200"],
    basic_auth=("elastic", "ITon0zhh"),
    verify_certs=False
)
INDEX_NAME = "documents"


class EnqueuedFile(BaseModel):
    id: str
    filename: str
    status: str

class MockDocument(BaseModel):
    filename: str
    text: str

@app.post("/mock-index")
async def seed_data(docs: List[MockDocument]):
    """
    Endpoint to manually push text into Elasticsearch for testing.
    """
    results = []
    for doc in docs:
        try:
            resp = es.index(index=INDEX_NAME, id=doc.filename, document=doc.model_dump())
            results.append({"filename": doc.filename, "result": resp['result']})
        except Exception as e:
            results.append({"filename": doc.filename, "error": str(e)})
            
    return {"status": "batch_processed", "details": results}

@app.get("/search")
async def search_documents(q: Optional[str] = None):
    """
    Search for text within the indexed documents.
    """
    if not q:
        raise HTTPException(status_code=400, detail="Query parameter 'q' is required")

    try:
        query = {
            "query": {
                "bool": {
                    "should": [
                        {"match": {"text": {"query": q, "boost": 5.0}}},
                        {"match": {"text.prefix": {"query": q, "boost": 2.0}}},
                        {"wildcard": {"text.raw": {"value": f"*{q}*", "boost": 1.0}}}
                    ]
                }
            },
            "highlight": {
                "require_field_match": False,
                "fields": {
                    "text": {
                        "pre_tags": ["<em>"],
                        "post_tags": ["</em>"],
                        "fragment_size": 150,
                        "number_of_fragments": 3
                    }
                }
            }
        }
        
        response = es.search(index=INDEX_NAME, body=query)
        
        hits = []
        for hit in response['hits']['hits']:
            hits.append({
                "filename": hit["_source"]["filename"],
                "score": hit["_score"],
                "snippets": hit.get("highlight", {}).get("text", [])
            })

        return {
            "total_results": response['hits']['total']['value'],
            "results": hits
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/process-document", status_code=202, response_model=EnqueuedFile)
async def process_document(file: UploadFile = File(...)):
    file_path = UPLOAD_DIR / file.filename

    try:
        content = await file.read()

        with open(file_path, "wb") as f:
            f.write(content)

        task = apply_ocr.delay(file_path)
        
        return EnqueuedFile(
            id=task.id,
            filename=file.filename,
            status="Processing document..."
        )
    
    except Exception as exc:
        print(f"Error saving file: {exc}")
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to save document: {str(exc)}"
        )
    
@app.post("/process-document-now")
async def process_document_now(file: UploadFile):
    filename = file.filename
    file_path = UPLOAD_DIR / filename

    try:
        content = await file.read()

        with open(file_path, "wb") as f:
            f.write(content)

        document_pages_as_images = convert_from_path(file_path)
        if not document_pages_as_images:
            raise ValueError("No pages were extracted from the PDF.")

        full_text = []

        for page in document_pages_as_images:
            page_text = pytesseract.image_to_string(page, lang="por")
            full_text.append(page_text)

        combined_text = "\n".join(full_text)

        es.index(
            index=INDEX_NAME,
            id=file.filename,
            document={
                "filename": filename,
                "text": combined_text
            }
        )

        return {
            "filename": filename,
            "pages_processed": len(document_pages_as_images)
        }

    except Exception as exc:
        print(f"Error saving file: {exc}")
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to save document: {str(exc)}"
        )
    
    # content = await file.read()
    # s3_client = get_s3_client()

    # try:
    #     create_bucket_if_not_exists(s3_client, BUCKET_NAME)

    #     file_obj = io.BytesIO(content)
    #     s3_client.upload_fileobj(file_obj, BUCKET_NAME, file.filename)

    #     task = apply_ocr.delay(file.filename)

    #     enqueued_file = EnqueuedFile(
    #         id=task.id,
    #         filename=file.filename,
    #         status="Processing document..."
    #     )

    #     return enqueued_file
    # except Exception as exc:
    #     return {
    #         "status": "Failed to save document",
    #         "error": exc
    #     }

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
