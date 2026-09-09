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
