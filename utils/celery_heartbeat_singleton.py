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


class HeartbeatingSingleton(Singleton):
    abstract = True

    lock_expiry = SINGLETON_LOCK_EXPIRY
    renewal_interval = SINGLETON_LOCK_RENEWAL_INTERVAL

    _renew_script = None
    _release_script = None
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
        # Renew immediately, not after the first renewal_interval wait: the
        # lock's TTL starts counting down at dispatch time (apply_async), not
        # at execution time, so a task that sat queued for a while before a
        # worker picked it up could otherwise have its lock expire before the
        # first scheduled renewal ever fires.
        self._renew_lock(lock, task_id)

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

        self._start_heartbeat(lock, task_id)
        try:
            return super().__call__(*args, **kwargs)
        finally:
            self._stop_heartbeat()
