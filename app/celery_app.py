from celery import Celery
from paddleocr import PaddleOCR

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
def save_in_object_storage(
    filename: str,
):
    result = ocr.predict(input="https://paddle-model-ecology.bj.bcebos.com/paddlex/imgs/demo_image/general_ocr_002.png")
    for res in result:
        res.print()
    
    print("2. Step 1")

    for item in res.doc_res:
            print(f"Detected Text: {item['text']} | Confidence: {item['score']}")

    print("3. Step 2")

    print("4. Processed")
    return
