from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import QueuePool

from utils.config import settings

DATABASE_URL = settings.env.DATABASE_URL

#Dynamic pool sizing based on Celery concurrency
CELERY_CONCURRENCY = settings.env.CELERY_CONCURRENCY

MAX_TOTAL_CONNECTIONS = 90  # Leave room for webconnections

# Calculate safe pool_size and max_overflow per worker
per_worker_limit = max(1, MAX_TOTAL_CONNECTIONS // CELERY_CONCURRENCY)
pool_size = max(1, per_worker_limit // 2)
max_overflow = max(0, per_worker_limit - pool_size)

# Configure connection pool for production use
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=pool_size,       # Number of connections to maintain
    max_overflow=max_overflow, # Additional connections allowed under load
    pool_pre_ping=True,        # Verify connections before using them
    pool_recycle=3600,         # Recycle connections after 1 hour
    pool_timeout=30,           # Wait up to 30s for available connection
    echo=False,                # Set to True for SQL query logging in dev
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

@contextmanager
def get_db_session():
    """
    Context manager for database sessions with proper transaction handling.
    Commits on success, rolls back on exception.
    """
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
