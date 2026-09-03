"""Tests for utils/celery_heartbeat_singleton.py (MAIT-387 follow-up).

celery_singleton.Singleton's lock is set once at dispatch and only released
by on_success/on_failure. A fixed TTL is therefore always a ceiling on task
runtime. HeartbeatingSingleton keeps a short TTL but renews it periodically
while the task is alive, so a leaked lock (worker actually dead, renewals
stopped) still self-heals quickly, while a healthy long-running task is never
cut off mid-run.

The renewal/release must be ownership-checked (only touch the lock if its
current value is still this task's own task_id) — see the "Ownership race"
section of PR96-fixes.md for why an unconditional EXPIRE/DELETE is unsafe
once renewal is in play: an old task could otherwise extend or delete a
newer task's lock.

_renew_lock takes (lock, task_id) explicitly rather than reading self.request
internally, because self.request is backed by Celery's *thread-local*
request stack — a background heartbeat thread can't see the request the main
thread pushed. An earlier version read self.request from inside _renew_lock,
which passed every unit test (all called from the main thread) while being
completely broken under real Celery execution (renewal always saw
task_id=None and silently no-opped forever). TestRealHeartbeatThread below
exercises an actual background thread to catch exactly that class of bug.
"""

import threading
import time
import unittest
from unittest.mock import MagicMock

from celery.app.task import Context
from celery.utils.threads import LocalStack

from utils.celery_heartbeat_singleton import HeartbeatingSingleton


def _make_task(redis_client, task_id="task-a", lock="lock:key"):
    """Build a HeartbeatingSingleton instance wired to a fake redis client,
    without going through Celery's real task registration machinery.

    task.request is a read-only property backed by task.request_stack, which
    is normally set up when a task is bound to a Celery app via @app.task(...)
    — since we're deliberately bypassing that to unit test the lock logic in
    isolation, wire up a minimal stack by hand instead of push_request (which
    itself needs an existing request_stack to push onto).
    """
    task = HeartbeatingSingleton()
    task.request_stack = LocalStack()
    task.request_stack.push(Context(id=task_id, args=(), kwargs={}))
    task._singleton_backend = MagicMock()
    task._singleton_backend.redis = redis_client
    task.generate_lock = MagicMock(return_value=lock)
    return task


class TestRenewScript(unittest.TestCase):
    """The renew Lua script must only extend the TTL if the key still holds
    the given task_id — never unconditionally.
    """

    def test_renew_extends_when_still_owner(self):
        redis_client = MagicMock()
        renew_script = MagicMock(return_value=1)
        redis_client.register_script.return_value = renew_script

        task = _make_task(redis_client)
        task._renew_lock("lock:key", "task-a")

        renew_script.assert_called_once_with(keys=["lock:key"], args=["task-a", task.lock_expiry])

    def test_renew_is_noop_when_lock_owned_by_another_task(self):
        # The script itself enforces this server-side (GET == ARGV[1] check);
        # here we assert the call shape passes the given task_id, so a
        # different owner's key is correctly left alone by the script logic.
        redis_client = MagicMock()
        renew_script = MagicMock(return_value=0)  # script reports "not owner"
        redis_client.register_script.return_value = renew_script

        task = _make_task(redis_client)
        result = task._renew_lock("lock:key", "task-a")

        self.assertFalse(result)
        renew_script.assert_called_once_with(keys=["lock:key"], args=["task-a", task.lock_expiry])

    def test_renew_logs_warning_when_ownership_lost(self):
        redis_client = MagicMock()
        renew_script = MagicMock(return_value=0)
        redis_client.register_script.return_value = renew_script

        task = _make_task(redis_client)
        with self.assertLogs("utils.celery_heartbeat_singleton", level="WARNING") as logs:
            task._renew_lock("lock:key", "task-a")

        self.assertTrue(any("lock:key" in message for message in logs.output))

    def test_renew_survives_a_redis_error_without_raising(self):
        # A transient Redis error must not propagate out of _renew_lock: that
        # would kill the heartbeat thread's loop, silently ending all future
        # renewals for the rest of the task's run.
        redis_client = MagicMock()
        renew_script = MagicMock(side_effect=ConnectionError("redis unreachable"))
        redis_client.register_script.return_value = renew_script

        task = _make_task(redis_client)
        with self.assertLogs("utils.celery_heartbeat_singleton", level="ERROR") as logs:
            result = task._renew_lock("lock:key", "task-a")

        self.assertFalse(result)
        self.assertTrue(any("lock:key" in message for message in logs.output))

    def test_heartbeat_loop_keeps_retrying_after_a_renewal_error(self):
        # One failed renewal tick must not stop the loop from trying again on
        # the next interval.
        redis_client = MagicMock()
        call_count = {"n": 0}

        def flaky_script(keys, args):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise ConnectionError("transient")
            return 1

        redis_client.register_script.return_value = flaky_script

        task = _make_task(redis_client)
        task.renewal_interval = 0.02
        task.run = MagicMock(side_effect=lambda: time.sleep(0.1))
        task()

        self.assertGreaterEqual(call_count["n"], 3)  # survived the failed 2nd call


class TestReleaseScript(unittest.TestCase):
    """Release (replacing celery_singleton's unconditional unlock/delete) must
    also be ownership-checked, for the same reason as renewal.

    unlock() takes task_id explicitly (falling back to self.request.id only
    when omitted) rather than always reading self.request internally. That
    matters because celery_singleton.Singleton.lock_and_run calls
    `self.unlock(lock)` (no task_id) from its own dispatch-time cleanup path
    — inside apply_async, before push_request — where self.request would not
    be the dispatching task's real request. on_success/on_failure, by
    contrast, receive their own task_id as a real parameter from Celery and
    must forward it explicitly (TestReleaseLockChain below), not rely on
    self.request happening to still be valid.
    """

    def test_release_deletes_when_still_owner_via_explicit_task_id(self):
        redis_client = MagicMock()
        release_script = MagicMock(return_value=1)
        redis_client.register_script.return_value = release_script

        task = _make_task(redis_client)
        task.unlock("lock:key", task_id="task-a")

        release_script.assert_called_once_with(keys=["lock:key"], args=["task-a"])

    def test_release_does_not_delete_lock_owned_by_newer_task(self):
        # This is the exact race from the plan: task-a's lock already expired,
        # task-b acquired the same key, then task-a finishes and tries to
        # release. task-a's release must be a no-op, not delete task-b's lock.
        redis_client = MagicMock()
        release_script = MagicMock(return_value=0)
        redis_client.register_script.return_value = release_script

        task = _make_task(redis_client)
        task.unlock("lock:key", task_id="task-a")

        release_script.assert_called_once_with(keys=["lock:key"], args=["task-a"])

    def test_release_falls_back_to_self_request_id_when_task_id_omitted(self):
        # Compatibility path for celery_singleton's own `self.unlock(lock)`
        # call site, which passes no task_id.
        redis_client = MagicMock()
        release_script = MagicMock(return_value=1)
        redis_client.register_script.return_value = release_script

        task = _make_task(redis_client, task_id="task-a")
        task.unlock("lock:key")

        release_script.assert_called_once_with(keys=["lock:key"], args=["task-a"])


class TestReleaseLockChain(unittest.TestCase):
    """on_success/on_failure must forward their own task_id parameter all the
    way through release_lock() to unlock(), not rely on self.request.id.

    Regression coverage for the same class of bug as the renewal fix: Celery
    hands on_success/on_failure the correct task_id directly, and
    celery_singleton's own release_lock() silently drops it — forward it
    explicitly instead of trusting self.request to be the right request at
    the point unlock() runs.
    """

    def test_on_success_forwards_its_own_task_id_to_unlock(self):
        redis_client = MagicMock()
        release_script = MagicMock(return_value=1)
        redis_client.register_script.return_value = release_script

        # Task instance's own request is a *different* id than the one
        # on_success is told about, to prove on_success's argument wins.
        task = _make_task(redis_client, task_id="stale-request-id")
        task.on_success("retval", "task-a", (), {})

        release_script.assert_called_once_with(keys=["lock:key"], args=["task-a"])

    def test_on_failure_forwards_its_own_task_id_to_unlock(self):
        redis_client = MagicMock()
        release_script = MagicMock(return_value=1)
        redis_client.register_script.return_value = release_script

        task = _make_task(redis_client, task_id="stale-request-id")
        task.on_failure(RuntimeError("boom"), "task-a", (), {}, None)

        release_script.assert_called_once_with(keys=["lock:key"], args=["task-a"])


class TestHeartbeatLifecycle(unittest.TestCase):
    """The renewal thread must start when the task body runs and stop when it
    finishes, on both the success and the exception path.
    """

    def test_heartbeat_starts_and_stops_on_success(self):
        redis_client = MagicMock()
        redis_client.register_script.return_value = MagicMock(return_value=1)
        task = _make_task(redis_client)

        started = []
        stopped = []
        task._start_heartbeat = lambda lock, task_id: started.append((lock, task_id))
        task._stop_heartbeat = lambda: stopped.append(True)
        task.run = MagicMock(return_value="ok")

        result = task()

        self.assertEqual(result, "ok")
        self.assertEqual(started, [("lock:key", "task-a")])
        self.assertEqual(stopped, [True])

    def test_heartbeat_stops_even_when_task_raises(self):
        redis_client = MagicMock()
        redis_client.register_script.return_value = MagicMock(return_value=1)
        task = _make_task(redis_client)

        stopped = []
        task._start_heartbeat = MagicMock()
        task._stop_heartbeat = lambda: stopped.append(True)
        task.run = MagicMock(side_effect=RuntimeError("boom"))

        with self.assertRaises(RuntimeError):
            task()

        self.assertEqual(stopped, [True])


class TestRealHeartbeatThread(unittest.TestCase):
    """Exercises the actual background thread (not a stub), because
    self.request is thread-local: a version that reads self.request from
    inside the renewal loop passes every test above (all main-thread calls)
    while being silently broken for real — renewal always sees task_id=None
    and never actually extends the lock. These tests must NOT stub
    _start_heartbeat/_renew_lock.
    """

    def test_heartbeat_thread_renews_with_correct_task_id(self):
        redis_client = MagicMock()
        renew_calls = []

        def fake_script(keys, args):
            renew_calls.append((keys, args))
            return 1

        redis_client.register_script.return_value = fake_script

        task = _make_task(redis_client, task_id="task-a", lock="lock:key")
        task.renewal_interval = 0.02  # fast for the test

        task.run = MagicMock(side_effect=lambda: time.sleep(0.1))
        task()

        self.assertGreaterEqual(len(renew_calls), 2)  # immediate renew + at least one interval tick
        for keys, args in renew_calls:
            self.assertEqual(keys, ["lock:key"])
            self.assertEqual(args[0], "task-a")  # not None — proves the thread saw the real task_id

    def test_heartbeat_thread_stops_when_task_finishes(self):
        redis_client = MagicMock()
        redis_client.register_script.return_value = MagicMock(return_value=1)

        task = _make_task(redis_client)
        task.renewal_interval = 0.02

        task.run = MagicMock(return_value="ok")
        task()

        # After __call__ returns, no heartbeat thread should still be running.
        self.assertIsNone(task._heartbeat_thread)
        other_threads = [t for t in threading.enumerate() if t is not threading.current_thread()]
        heartbeat_threads = [t for t in other_threads if t.name.startswith("Thread") and t.is_alive()]
        # Give a stray thread a brief moment to notice the stop signal, if any.
        for t in heartbeat_threads:
            t.join(timeout=0.5)
        self.assertFalse(any(t.is_alive() for t in heartbeat_threads))
