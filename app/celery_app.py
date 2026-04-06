from celery import Celery
from app.s3_client import get_s3_client
from pdf2image import convert_from_bytes
import pytesseract
from elasticsearch import Elasticsearch

BUCKET_NAME = "documents"
OUTPUT_DIR = "ocr_output"

es = Elasticsearch(
    ["http://localhost:9200"],
    basic_auth=("elastic", "ITon0zhh"),
    verify_certs=False
)
INDEX_NAME = "documents"

celery_app = Celery(
    "worker",
    broker="amqp://arqcabe:rabbitmq_password@localhost:5672/arqcabe_vhost"
)


@celery_app.task(name="apply_ocr")
def apply_ocr(filename: str):
    try:
        document_bytes = get_pdf(filename)
        if not document_bytes:
            raise ValueError("The retrieved document is empty.")
            
        # Point pdf2image to the specific bin folder you found
        document_pages_as_images = convert_from_bytes(
            document_bytes,
            poppler_path=r"C:\poppler\poppler-25.12.0\Library\bin"
        )
        
        full_text = []

        for index, page in enumerate(document_pages_as_images):
            # Ensure Tesseract is also working (see note below)
            page_text = pytesseract.image_to_string(page, lang="por")
            full_text.append(page_text)

        combined_text = "\n".join(full_text)

        es.index(
            index=INDEX_NAME,
            id=filename,
            document={
                "filename": filename,
                "text": combined_text
            }
        )

        return {
            "status": "completed",
            "filename": filename,
            "pages_processed": len(document_pages_as_images)
        }

    except Exception as e:
        return {
            "status": "failed",
            "error": str(e)
        }

def get_pdf(filename: str):
    s3_client = get_s3_client()
    try:
        s3_object = s3_client.get_object(Bucket=BUCKET_NAME, Key=filename)
        data = s3_object["Body"].read()
        
        print(f"DEBUG: Retrieved {len(data)} bytes for {filename}")
        print(f"DEBUG: First 5 bytes: {data[:5]}") # PDFs should start with b'%PDF-'
        
        return data
    except Exception as e:
        print(f"Error fetching PDF from S3: {e}")
        return None
