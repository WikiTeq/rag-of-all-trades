from __future__ import annotations

# TTL (seconds) for a task's celery_singleton lock. Deliberately short and
# independent of the task's own schedule interval: with HeartbeatingSingleton
# (utils/celery_heartbeat_singleton.py) the running task renews this TTL
# periodically for as long as it's alive, so a *healthy* task can run
# arbitrarily long without its lock expiring. This TTL only governs how fast
# a *leaked* lock (worker killed mid-task, renewals stopped) self-heals.
SINGLETON_LOCK_EXPIRY = 180

# How often a running task renews its own lock. Must be comfortably shorter
# than SINGLETON_LOCK_EXPIRY so a renewal has room to land before the lock
# would otherwise expire.
SINGLETON_LOCK_RENEWAL_INTERVAL = 60
