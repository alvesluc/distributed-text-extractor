from celery import Celery
from pdf2image import convert_from_bytes, convert_from_path
import pytesseract
from elasticsearch import Elasticsearch
from pathlib import Path

UPLOAD_DIR = Path("uploaded_documents")
INDEX_NAME = "documents"

es = Elasticsearch(
    ["http://elasticsearch:9200"],
    basic_auth=("elastic", "ITon0zhh"),
    verify_certs=False
)

celery_app = Celery(
    "worker",
    broker="amqp://arqcabe:rabbitmq_password@localhost:5672/arqcabe_vhost"
)


@celery_app.task(name="apply_ocr")
def apply_ocr(file_path: str):
    """
    file_path: The full string path to the local file.
    """
    try:
        path_obj = Path(file_path)
        
        if not path_obj.exists():
            raise FileNotFoundError(f"File not found at: {file_path}")

        # convert_from_path is better than reading bytes into memory first
        document_pages_as_images = convert_from_path(
            file_path,
            poppler_path=r"C:\poppler\poppler-25.12.0\Library\bin"
        )
        
        if not document_pages_as_images:
            raise ValueError("No pages were extracted from the PDF.")
        
        full_text = []

        for page in document_pages_as_images:
            page_text = pytesseract.image_to_string(page, lang="por")
            full_text.append(page_text)

        combined_text = "\n".join(full_text)

        # Use filename (basename) as the ES ID, but keep the path for logic
        filename = path_obj.name

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
        print(f"Error during OCR processing: {e}")
        return {
            "status": "failed",
            "error": str(e)
        }


    #     document_bytes = get_pdf(filename)
    #     if not document_bytes:
    #         raise ValueError("The retrieved document is empty.")
            
    #     document_pages_as_images = convert_from_bytes(
    #         document_bytes,
    #         poppler_path=r"C:\poppler\poppler-25.12.0\Library\bin"
    #     )
        
    #     full_text = []

    #     for index, page in enumerate(document_pages_as_images):
    #         page_text = pytesseract.image_to_string(page, lang="por")
    #         full_text.append(page_text)

    #     combined_text = "\n".join(full_text)

    #     es.index(
    #         index=INDEX_NAME,
    #         id=filename,
    #         document={
    #             "filename": filename,
    #             "text": combined_text
    #         }
    #     )

    #     return {
    #         "status": "completed",
    #         "filename": filename,
    #         "pages_processed": len(document_pages_as_images)
    #     }

    # except Exception as e:
    #     return {
    #         "status": "failed",
    #         "error": str(e)
    #     }

# def get_pdf(filename: str):
#     s3_client = get_s3_client()
#     try:
#         s3_object = s3_client.get_object(Bucket=BUCKET_NAME, Key=filename)
#         data = s3_object["Body"].read()
        
#         print(f"DEBUG: Retrieved {len(data)} bytes for {filename}")
#         print(f"DEBUG: First 5 bytes: {data[:5]}") # PDFs should start with b'%PDF-'
        
#         return data
#     except Exception as e:
#         print(f"Error fetching PDF from S3: {e}")
#         return None
