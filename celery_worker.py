import os

from celery import Celery
from celery.utils.log import get_task_logger

BROKER_URL = os.getenv("CELERY_BROKER_URL", "amqp://guest:guest@localhost:5672//")
RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "rpc://")
MAX_RETRIES = int(os.getenv("CELERY_MAX_RETRIES", "4"))
QUEUE_NAME = os.getenv("CELERY_QUEUE_NAME", "doc_conversions")

celery_app = Celery("doc_conversion", broker=BROKER_URL, backend=RESULT_BACKEND)
celery_app.conf.update(
    task_default_queue=QUEUE_NAME,
    worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,
)

logger = get_task_logger(__name__)


@celery_app.task(
    bind=True,
    name="doc_conversion.convert_doc",
    autoretry_for=(RuntimeError,),
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=True,
    max_retries=MAX_RETRIES,
)
def convert_doc_task(self, doc_id: int):
    """
    Celery task that runs generate_docs_for_soff(doc_id) and retries with exponential
    backoff when the conversion fails the first time.
    """
    from new_docx2pdf import generate_docs_for_soff

    logger.info("Starting conversion for doc_id=%s", doc_id)
    success = generate_docs_for_soff(doc_id)
    if not success:
        raise RuntimeError(f"Conversion failed for doc_id={doc_id}")

    logger.info("doc_id=%s conversion completed", doc_id)
    return {"doc_id": doc_id, "status": "completed"}


def enqueue_doc_id(doc_id: int):
    """
    Convenience helper to enqueue a doc_id from existing scripts:
        from celery_worker import enqueue_doc_id
        enqueue_doc_id(123)
    """
    return convert_doc_task.apply_async(args=(doc_id,))

