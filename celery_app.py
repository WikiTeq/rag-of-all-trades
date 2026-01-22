import logging
from celery import Celery
from celery.signals import worker_process_init, worker_process_shutdown
from celery_singleton import Singleton
from utils.config import settings
from utils.db import engine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

celery_app = Celery(
    "rag_ingestion",
    broker=settings.env.REDIS_URL,
    backend=None,
)
# Task execution
celery_app.conf.worker_prefetch_multiplier = 1
celery_app.conf.task_acks_late = True
celery_app.conf.task_reject_on_worker_lost = True

# Disable unnecessary tracking
celery_app.conf.task_track_started = False
celery_app.conf.task_send_sent_event = False

# Beat scheduler
celery_app.conf.beat_scheduler = "redbeat.RedBeatScheduler"
celery_app.conf.redbeat_redis_url = settings.env.REDIS_URL
celery_app.conf.redbeat_lock_timeout = 60
celery_app.conf.redbeat_key_prefix = "redbeat:"
celery_app.conf.beat_sync_every = 10
celery_app.conf.beat_max_loop_interval = 10
celery_app.conf.beat_schedule = {}

# see https://ryan-zheng.medium.com/solving-sqlalchemy-connection-issues-in-celery-workers-9d7cbf299221
# see https://www.yangster.ca/post/not-the-same-pre-fork-worker-model/
# Reset DB connections per worker process
@worker_process_init.connect
def init_worker(**kwargs):
    engine.dispose()

@worker_process_shutdown.connect
def shutdown_worker(**kwargs):
    engine.dispose()

def create_task_for_source(source_config):
    account_name = source_config["name"]
    bucket_name = source_config["config"].get("bucket_override")

    if bucket_name:
        task_name = f"{source_config['type']}_ingest_{account_name}_{bucket_name}"
    else:
        task_name = f"{source_config['type']}_ingest_{account_name}"

    @celery_app.task(
        name=task_name,
        base=Singleton,
        ignore_result=True,
        bind=True
    )
    def run_source(self, pipeline_config=source_config):
        from tasks.factory import IngestionJobFactory
        bucket_override = pipeline_config["config"].get("bucket_override")
        log_name = f"{pipeline_config['name']}_{bucket_override}" if bucket_override else pipeline_config['name']
        logger.info(f"Starting ingestion for {log_name}")
        
        job = IngestionJobFactory.create(
            pipeline_config["type"],
            pipeline_config
        )
        return job.run()

    # Register task in Beat schedule
    celery_app.conf.beat_schedule[task_name] = {
        "task": task_name,
        "schedule": source_config.get("schedule"),
        "args": ()
    }

# Register all sources
try:
    for source in settings.SOURCES:
        create_task_for_source(source)
except Exception as e:
    logger.exception("Failed during source task registration")
    raise
