# from celery import Celery
# import pytesseract
# from PIL import Image
# import os
# import json

# celery_app = Celery(
#     'worker',
#     broker='amqp://arqcabe:rabbitmq_password@localhost:5672/arqcabe_vhost'
# )

# @celery_app.task(name="apply_ocr")
# def apply_ocr(filename: str):
#     BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
#     image_path = os.path.join(BASE_DIR, "images", "sl-bem-298-66510c4b323cf-66510c4b34d37_page-0001.jpg")
    
#     if not os.path.exists(image_path):
#         print(f"Error: File not found at {image_path}")
#         return

#     try:
#         img = Image.open(image_path)
#         data = pytesseract.image_to_data(img, lang='por', output_type=pytesseract.Output.DICT)

#         print("OCR Results:")
#         print(data['text'])

#     except Exception as e:
#         print(f"Error during OCR processing: {e}")
#         return

#     print("2. Processed")
#     return


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
    s3_client = get_s3_client()

    try:
        s3_object = s3_client.get_object(Bucket=BUCKET_NAME, Key=filename)
        pdf_bytes = s3_object["Body"].read()

        pages = convert_from_bytes(pdf_bytes)

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        base_name = os.path.splitext(filename)[0]

        for index, page in enumerate(pages, start=1):
            text = pytesseract.image_to_string(page, lang="por")

            output_file_path = os.path.join(
                OUTPUT_DIR,
                f"{base_name}_page_{index}.txt"
            )

            with open(output_file_path, "w", encoding="utf-8") as f:
                f.write(text)

        return {
            "status": "completed",
            "pages_processed": len(pages)
        }

    except Exception as e:
        return {
            "status": "failed",
            "error": str(e)
        }