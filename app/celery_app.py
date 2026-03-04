from celery import Celery
from app.s3_client import get_s3_client
from pdf2image import convert_from_bytes
import pytesseract
import os

BUCKET_NAME = "documents"
OUTPUT_DIR = "ocr_output"

celery_app = Celery(
    "worker",
    broker="amqp://arqcabe:rabbitmq_password@localhost:5672/arqcabe_vhost"
)


@celery_app.task(name="apply_ocr")
def apply_ocr(filename: str):
    try:
        document_bytes = get_pdf(filename)
        document_pages_as_images = convert_from_bytes(document_bytes)

        full_text = []

        for index, page in enumerate(document_pages_as_images):
            page_text = pytesseract.image_to_string(page, lang="por")
            full_text.append(page_text)

        # Save to elasticsearch here

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
        return s3_object["Body"].read()
    except Exception as e:
        print(f"Error fetching PDF from S3: {e}")
        return None
