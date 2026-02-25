from celery import Celery

celery_app = Celery(
    'worker',
    broker='amqp://arqcabe:rabbitmq_password@localhost:5672/arqcabe_vhost'
)


@celery_app.task()
def save_in_object_storage(
    filename: str,
):
    print("Success: {filename} was saved.")
