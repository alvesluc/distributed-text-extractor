from celery import Celery
from paddleocr import PaddleOCR
import os

celery_app = Celery(
    'worker',
    broker='amqp://arqcabe:rabbitmq_password@localhost:5672/arqcabe_vhost'
)

ocr = PaddleOCR(
    use_doc_orientation_classify=False,
    use_doc_unwarping=False,
    use_textline_orientation=False,
    enable_mkldnn=False
)

@celery_app.task(name="save_in_object_storage")
def save_in_object_storage(filename: str):
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    image_path = os.path.join(BASE_DIR, "images", "sl-bem-298-66510c4b323cf-66510c4b34d37_page-0001.jpg")
    
    if not os.path.exists(image_path):
        print(f"Error: File not found at {image_path}")
        return

    result = ocr.predict(image_path)
    
    print("1. OCR Started")

    for line in result:
        for res in line:
            text = res[1][0]
            confidence = res[1][1]
            print(f"Detected Text: {text} | Confidence: {confidence}")

    print("2. Processed")
    return
