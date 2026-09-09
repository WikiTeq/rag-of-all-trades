"""MAIT-387 follow-up: a Singleton task base whose lock TTL is renewed
periodically while the task is alive, instead of being a fixed ceiling on
task runtime.

celery_singleton.Singleton sets its Redis lock once at dispatch (SET key
task_id NX EX=lock_expiry) and only ever releases it via on_success/
on_failure. Any fixed lock_expiry is therefore a ceiling on total task
runtime: a healthy task that runs longer than the TTL has its lock expire
out from under it, letting Beat dispatch a second, overlapping run of the
same task (https://github.com/WikiTeq/rag-of-all-trades/pull/96#discussion_r3925988356).

HeartbeatingSingleton keeps SINGLETON_LOCK_EXPIRY short (fast recovery from a
genuinely leaked lock — worker killed mid-task) and instead has the running
task renew its own lock's TTL every SINGLETON_LOCK_RENEWAL_INTERVAL seconds
for as long as it's alive. A leaked lock (renewals stopped because the
worker died) still self-heals within roughly one TTL window; a healthy task
can run arbitrarily long.

Renewal and release are ownership-checked via Lua scripts (atomic
GET-then-EXPIRE / GET-then-DELETE, gated on the lock's value still being this
task's own task_id) rather than celery_singleton's unconditional
EXPIRE/DELETE. Without this, once renewal is in play, a task whose lock
already expired and got reacquired by a newer dispatch could renew — or on
finishing, delete — that newer task's lock instead of its own.

Start-of-run ownership uses one atomic renew-or-reacquire-or-skip check
(_start_lock / _START_SCRIPT) instead of a separate GET-then-EXPIRE — see
_start_lock's docstring for the race a two-step check would have, and why
"lock missing" must mean "reacquire," not "skip." lock_and_run is also
overridden (copied from celery_singleton, not just monkeypatched) to fix a
dispatch-time cleanup bug — see its own docstring for why.
"""

import logging
import threading

from celery_singleton import Singleton

from utils.celery_scheduling import SINGLETON_LOCK_EXPIRY, SINGLETON_LOCK_RENEWAL_INTERVAL

logger = logging.getLogger(__name__)

# KEYS[1] = lock key, ARGV[1] = this task's own task_id, ARGV[2] = new TTL (renew only)
_RENEW_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("EXPIRE", KEYS[1], ARGV[2])
else
    return 0
end
"""

# KEYS[1] = lock key, ARGV[1] = this task's own task_id
_RELEASE_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""

# Atomic start-of-run check: renew if this task_id already owns the lock,
# reacquire if the lock is missing entirely (nobody owns it — e.g. this
# task sat queued past SINGLETON_LOCK_EXPIRY, or this is a same-task_id
# redelivery per task_acks_late/task_reject_on_worker_lost), or report
# "superseded" only if a *different* task_id currently holds it. Doing this
# as one Lua script (rather than a separate GET-check followed by a
# separate EXPIRE/SET call) closes the race where the lock changes state
# between those two round-trips.
#
# KEYS[1] = lock key, ARGV[1] = this task's own task_id, ARGV[2] = TTL
# Returns: 1 = renewed (already owned it), 2 = reacquired (was missing),
#          0 = superseded (a different task_id owns it — caller must skip)
_START_SCRIPT = """
local current = redis.call("GET", KEYS[1])
if current == ARGV[1] then
    redis.call("EXPIRE", KEYS[1], ARGV[2])
    return 1
elseif current == false then
    redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])
    return 2
else
    return 0
end
"""

_START_RENEWED = 1
_START_REACQUIRED = 2
_START_SUPERSEDED = 0


class HeartbeatingSingleton(Singleton):
    abstract = True

    lock_expiry = SINGLETON_LOCK_EXPIRY
    renewal_interval = SINGLETON_LOCK_RENEWAL_INTERVAL

    _renew_script = None
    _release_script = None
    _start_script = None
    _heartbeat_thread = None
    _heartbeat_stop = None

    def _get_script(self, attr_name, source):
        script = getattr(self, attr_name)
        if script is None:
            script = self.singleton_backend.redis.register_script(source)
            setattr(self, attr_name, script)
        return script

    def _renew_lock(self, lock, task_id):
        """Atomically extend the lock's TTL, only if it still belongs to
        task_id. Returns a truthy value if the renewal succeeded, falsy if
        task_id no longer owns the lock (already leaked to a newer dispatch).

        Takes lock/task_id explicitly rather than reading self.request:
        self.request is backed by Celery's *thread-local* request stack
        (celery.utils.threads.LocalStack), so a background heartbeat thread
        cannot see the request the main task thread pushed — it would always
        read the default empty Context (task_id=None), silently turning every
        renewal into a no-op. Capturing these values on the main thread
        before starting the heartbeat (see __call__) and passing them in is
        required, not just a style choice.
        """
        script = self._get_script("_renew_script", _RENEW_SCRIPT)
        try:
            renewed = script(keys=[lock], args=[task_id, self.lock_expiry])
        except Exception:
            # A transient Redis error must not kill the heartbeat thread —
            # that would silently stop all future renewals for the rest of
            # the task's run, reintroducing the original bug (lock expires
            # under a still-healthy task) after a delay instead of fixing it.
            # Log and let the next scheduled tick try again.
            logger.exception("Renewing lock %s for task %s failed", lock, task_id)
            return False
        if not renewed:
            logger.warning(
                "Lock %s no longer owned by task %s — skipping renewal (a newer dispatch may already be running)",
                lock,
                task_id,
            )
        return renewed

    def _start_lock(self, lock, task_id):
        """Atomically renew, reacquire, or detect supersession at the start
        of a run — see _START_SCRIPT above for the exact semantics.

        Returns True if the task should proceed (lock renewed or reacquired),
        False if a different task_id already owns the lock and this run must
        be skipped instead.
        """
        script = self._get_script("_start_script", _START_SCRIPT)
        try:
            result = script(keys=[lock], args=[task_id, self.lock_expiry])
        except Exception:
            # A transient Redis error here must not silently let the task run
            # unprotected (no confirmed ownership) or silently skip a
            # legitimate run — log and treat as "proceed" so a flaky Redis
            # call doesn't drop real work; the periodic heartbeat renewal
            # will still enforce ownership going forward once Redis recovers.
            logger.exception("Start-of-run lock check for %s (task %s) failed", lock, task_id)
            return True
        if result == _START_SUPERSEDED:
            logger.warning(
                "Lock %s no longer available for task %s at start "
                "(a different task_id already owns it) — skipping this run",
                lock,
                task_id,
            )
            return False
        if result == _START_REACQUIRED:
            logger.info(
                "Lock %s was missing for task %s at start (queued past TTL, "
                "or a same-task_id redelivery) — reacquiring and proceeding",
                lock,
                task_id,
            )
        return True

    def unlock(self, lock, task_id=None):
        """Ownership-checked release, replacing Singleton's unconditional
        DELETE. Only removes the lock if it still holds task_id — otherwise a
        task whose lock already expired and was reacquired by a newer
        dispatch would delete that newer task's lock.

        task_id is optional to stay call-compatible with
        celery_singleton.Singleton.lock_and_run's own cleanup path, which
        calls `self.unlock(lock)` with no task_id — that call happens at
        dispatch time (inside apply_async, before push_request), so
        self.request wouldn't be valid there anyway. When task_id is omitted,
        fall back to self.request.id for the on_success/on_failure path
        (release_lock below), which does run on the main task thread where
        self.request is valid.
        """
        if task_id is None:
            task_id = self.request.id
        script = self._get_script("_release_script", _RELEASE_SCRIPT)
        script(keys=[lock], args=[task_id])

    def release_lock(self, task_args=None, task_kwargs=None, task_id=None):
        """Same as celery_singleton.Singleton.release_lock, but forwards
        task_id through to the ownership-checked unlock() above instead of
        letting it fall back to self.request.id implicitly.
        """
        lock = self.generate_lock(self.name, task_args, task_kwargs)
        self.unlock(lock, task_id=task_id)

    def on_success(self, retval, task_id, args, kwargs):
        self.release_lock(task_args=args, task_kwargs=kwargs, task_id=task_id)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        self.release_lock(task_args=args, task_kwargs=kwargs, task_id=task_id)

    def _start_heartbeat(self, lock, task_id):
        # No immediate renewal here: __call__ already ran _start_lock (renew
        # or reacquire) atomically before calling this, so the lock is
        # already confirmed fresh for this task_id at this point. Doing a
        # second, separate renewal here would just be a redundant GET+EXPIRE
        # round-trip with no ownership benefit over what _start_lock already
        # established.
        self._heartbeat_stop = threading.Event()

        def _loop():
            while not self._heartbeat_stop.wait(self.renewal_interval):
                self._renew_lock(lock, task_id)

        self._heartbeat_thread = threading.Thread(target=_loop, daemon=True)
        self._heartbeat_thread.start()

    def _stop_heartbeat(self):
        if self._heartbeat_stop is not None:
            self._heartbeat_stop.set()
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=self.renewal_interval)
        self._heartbeat_thread = None
        self._heartbeat_stop = None

    def __call__(self, *args, **kwargs):
        # Capture on the main thread — self.request is only valid here, not
        # from the background heartbeat thread (see _renew_lock's docstring).
        lock = self.generate_lock(self.name, self.request.args, self.request.kwargs)
        task_id = self.request.id

        if not self._start_lock(lock, task_id):
            # A different task_id already owns this lock — this run has been
            # superseded (e.g. queued past SINGLETON_LOCK_EXPIRY and a newer
            # dispatch already claimed it). Skip without running the task
            # body or starting a heartbeat for a lock we don't hold.
            return None

        self._start_heartbeat(lock, task_id)
        try:
            return super().__call__(*args, **kwargs)
        finally:
            self._stop_heartbeat()

    def lock_and_run(self, lock, *args, task_id=None, **kwargs):
        """Copied from celery_singleton.Singleton.lock_and_run (installed
        version 0.3.1: .venv/lib/python3.13/site-packages/celery_singleton/singleton.py),
        with one change: the publish-failure cleanup below passes task_id
        through to unlock() explicitly, instead of upstream's bare
        `self.unlock(lock)`.

        Upstream's cleanup call drops task_id even though it's already in
        scope as this method's own parameter — self.unlock(lock) then falls
        through our unlock() override's task_id=None default, which reads
        self.request.id. But this call happens inside apply_async, before
        this task has any request context (dispatch time, not execution
        time), so that fallback doesn't reflect the id that was actually
        just written to the lock by aquire_lock() above. Without this
        override, a publish failure here would silently fail to release its
        own just-acquired lock — a leaked lock, the exact failure mode this
        whole fix exists to prevent.

        Re-check this method against celery_singleton's source on every
        version bump of that dependency.
        """
        lock_aquired = self.aquire_lock(lock, task_id)
        if lock_aquired:
            try:
                return super(Singleton, self).apply_async(*args, task_id=task_id, **kwargs)
            except Exception:
                self.unlock(lock, task_id=task_id)
                raise
