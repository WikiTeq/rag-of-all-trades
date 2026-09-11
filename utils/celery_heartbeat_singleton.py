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

Renewal and release are ownership-checked via Lua scripts (atomic, gated on
the lock's value still being this task's own task_id) rather than
celery_singleton's unconditional EXPIRE/DELETE. Without this, once renewal
is in play, a task whose lock already expired and got reacquired by a newer
dispatch could renew — or on finishing, delete — that newer task's lock
instead of its own.

Start-of-run and heartbeat renewal both use one shared atomic
renew-or-reacquire-or-skip check (_check_lock / _OWNERSHIP_SCRIPT) instead
of a plain GET-then-EXPIRE — see _check_lock's docstring for the race a
two-step check would have, why "lock missing" must mean "reacquire," not
"skip," and why a Redis error at start retries via Celery's own retry
mechanism instead of proceeding unprotected or requeuing at unbounded
speed (https://github.com/WikiTeq/rag-of-all-trades/pull/96#discussion_r3972417143).
lock_and_run is also overridden (copied from celery_singleton, not just
monkeypatched) to fix a dispatch-time cleanup bug — see its own docstring
for why.
"""

import logging
import threading

from celery_singleton import Singleton

from utils.celery_scheduling import (
    SINGLETON_LOCK_EXPIRY,
    SINGLETON_LOCK_RENEWAL_INTERVAL,
    SINGLETON_START_LOCK_MAX_RETRIES,
)

logger = logging.getLogger(__name__)

# KEYS[1] = lock key, ARGV[1] = this task's own task_id
_RELEASE_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""

# Shared atomic ownership check, used both at task start and by every
# heartbeat tick: renew if this task_id already owns the lock, reacquire if
# the lock is missing entirely (nobody owns it — e.g. this task sat queued
# past SINGLETON_LOCK_EXPIRY, this is a same-task_id redelivery per
# task_acks_late/task_reject_on_worker_lost, or the key was evicted/lost
# mid-run, e.g. a Redis restart), or report "superseded" only if a
# *different* task_id currently holds it. Doing this as one Lua script
# (rather than a separate GET-check followed by a separate EXPIRE/SET call)
# closes the race where the lock changes state between those two
# round-trips. The start check and the heartbeat renewal need the exact
# same three-way logic — a lock that goes missing mid-run (not just before
# the task starts) must also be reacquirable, not just renewable — so both
# call sites share this one script instead of keeping two near-duplicates.
#
# KEYS[1] = lock key, ARGV[1] = this task's own task_id, ARGV[2] = TTL
# Returns: 1 = renewed (already owned it), 2 = reacquired (was missing),
#          0 = superseded (a different task_id owns it — caller must skip)
_OWNERSHIP_SCRIPT = """
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

_LOCK_RENEWED = 1
_LOCK_REACQUIRED = 2
_LOCK_SUPERSEDED = 0


class HeartbeatingSingleton(Singleton):
    abstract = True

    lock_expiry = SINGLETON_LOCK_EXPIRY
    renewal_interval = SINGLETON_LOCK_RENEWAL_INTERVAL

    _ownership_script = None
    _release_script = None
    _heartbeat_thread = None
    _heartbeat_stop = None

    def _get_script(self, attr_name, source):
        script = getattr(self, attr_name)
        if script is None:
            script = self.singleton_backend.redis.register_script(source)
            setattr(self, attr_name, script)
        return script

    def _check_lock(self, lock, task_id):
        """Atomically renew, reacquire, or detect supersession of the lock —
        see _OWNERSHIP_SCRIPT above for the exact semantics. Used both at
        task start (_start_lock) and by every heartbeat tick (_renew_lock);
        those wrappers differ only in what they do with a Redis error, since
        a task that hasn't started yet can still retry the whole dispatch,
        while a task already mid-run cannot.

        Takes lock/task_id explicitly rather than reading self.request:
        self.request is backed by Celery's *thread-local* request stack
        (celery.utils.threads.LocalStack), so a background heartbeat thread
        cannot see the request the main task thread pushed — it would always
        read the default empty Context (task_id=None), silently turning every
        renewal into a no-op. Capturing these values on the main thread
        before starting the heartbeat (see __call__) and passing them in is
        required, not just a style choice.

        Raises whatever the underlying Redis call raises — callers decide
        how to handle a failure, this method does not swallow it.
        """
        script = self._get_script("_ownership_script", _OWNERSHIP_SCRIPT)
        return script(keys=[lock], args=[task_id, self.lock_expiry])

    def _renew_lock(self, lock, task_id):
        """Heartbeat-loop wrapper around _check_lock: renews or reacquires,
        returns a truthy value on success, falsy if a different task_id now
        owns the lock, and falsy (without raising) on a Redis error.

        A Redis error here must not kill the heartbeat thread — that would
        silently stop all future renewals for the rest of the task's run,
        reintroducing the original bug (lock expires under a still-healthy
        task) after a delay instead of fixing it. Unlike _start_lock, there
        is no "retry the dispatch" option mid-run — the task is already
        executing — so log and let the next scheduled tick try again instead.
        """
        try:
            result = self._check_lock(lock, task_id)
        except Exception:
            logger.exception("Renewing lock %s for task %s failed", lock, task_id)
            return False
        if result == _LOCK_SUPERSEDED:
            logger.warning(
                "Lock %s no longer owned by task %s — skipping renewal (a newer dispatch may already be running)",
                lock,
                task_id,
            )
            return False
        if result == _LOCK_REACQUIRED:
            logger.info(
                "Lock %s was missing for task %s mid-run (e.g. Redis restart) "
                "— reacquiring so this still-healthy task keeps its lock",
                lock,
                task_id,
            )
        return True

    def _start_lock(self, lock, task_id):
        """__call__ wrapper around _check_lock: renews or reacquires at task
        start, returning True if the task should proceed.

        Returns False only if a different task_id already owns the lock —
        this run has been genuinely superseded and must be skipped.

        On a Redis error, retries the whole dispatch via self.retry() rather
        than either proceeding unprotected (no confirmed ownership) or
        silently skipping a legitimate run. self.retry() raises Retry, which
        Celery's own trace machinery routes to on_retry — never on_failure —
        so this never releases a lock it doesn't hold; see the module
        docstring and PR96-fixes.md "Commit 3" for the full trace through
        Celery's source. countdown/max_retries give a transient blip (e.g. a
        brief Redis failover) room to clear before a genuinely sustained
        outage surfaces as a real failure, instead of an unbounded
        requeue-at-full-speed loop.
        """
        try:
            result = self._check_lock(lock, task_id)
        except Exception:
            logger.exception("Start-of-run lock check for %s (task %s) failed — retrying", lock, task_id)
            raise self.retry(
                countdown=SINGLETON_LOCK_RENEWAL_INTERVAL,
                max_retries=SINGLETON_START_LOCK_MAX_RETRIES,
            )
        if result == _LOCK_SUPERSEDED:
            logger.warning(
                "Lock %s no longer available for task %s at start "
                "(a different task_id already owns it) — skipping this run",
                lock,
                task_id,
            )
            return False
        if result == _LOCK_REACQUIRED:
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
