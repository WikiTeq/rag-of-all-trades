from __future__ import annotations

# TTL (seconds) for a task's celery_singleton lock. Deliberately independent
# of the task's own schedule interval: with HeartbeatingSingleton
# (utils/celery_heartbeat_singleton.py) the running task renews this TTL
# periodically for as long as it's alive, so a *healthy* task can run
# arbitrarily long without its lock expiring. This TTL mainly governs how
# fast a *leaked* lock (worker killed mid-task, renewals stopped) self-heals.
#
# Also covers Beat-queue backlog: the lock is written at dispatch (apply_async
# time), not when a worker actually calls the task, so a task that sits queued
# longer than this TTL before a worker picks it up would otherwise start with
# its lock already gone. HeartbeatingSingleton.__call__ handles that case
# safely (reacquire-or-skip, see its module docstring) rather than relying on
# this TTL alone, but a larger value still reduces how often that path
# triggers under normal queue backlog.
SINGLETON_LOCK_EXPIRY = 600

# How often a running task renews its own lock. Must be comfortably shorter
# than SINGLETON_LOCK_EXPIRY so a renewal has room to land before the lock
# would otherwise expire.
SINGLETON_LOCK_RENEWAL_INTERVAL = 60

# If Redis errors while a task is trying to confirm lock ownership at start
# (HeartbeatingSingleton._start_lock), the task retries via Celery's own
# retry mechanism rather than proceeding unprotected or requeuing at full
# speed. countdown=SINGLETON_LOCK_RENEWAL_INTERVAL gives a transient blip
# (e.g. a brief Redis failover) time to clear between attempts;
# max_retries=30 means ~30 minutes of retrying — comfortably longer than a
# normal Redis restart/failover, short enough that a genuinely sustained
# outage surfaces as a real task failure within the hour instead of
# retrying silently for days.
SINGLETON_START_LOCK_MAX_RETRIES = 30
