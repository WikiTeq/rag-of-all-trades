from __future__ import annotations

# Floor for a task's Singleton lock TTL: keeps very frequent schedules (e.g. 60s)
# from getting an unreasonably short lock.
MIN_SINGLETON_LOCK_EXPIRY = 300


def singleton_lock_expiry_for_schedule(schedule_seconds: int) -> int:
    """TTL (seconds) for a task's celery_singleton lock, derived from its own schedule interval.

    celery_singleton never releases its Redis lock if a worker is killed mid-task
    (e.g. `docker compose down`) — on_success/on_failure never fire, so the lock
    is left dangling with no expiry and the task is silently skipped forever
    afterwards (MAIT-387). Bounding the lock to roughly two schedule intervals
    means a leaked lock self-heals within a couple of missed runs, while still
    leaving enough headroom that a healthy task taking longer than a single
    interval doesn't have its lock expire out from under it — which would let
    Beat dispatch a second, overlapping run of the same task.
    """
    return max(int(schedule_seconds) * 2, MIN_SINGLETON_LOCK_EXPIRY)
