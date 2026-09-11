"""Bounded physical inference workers; queued deadlines never become GPU work."""
from concurrent.futures import Future
import math
import queue
import threading
import time


class LaneBusy(RuntimeError):
    pass


class DeadlineExpired(TimeoutError):
    pass


class WorkLane:
    def __init__(self, name, *, workers, pending, on_stalled, stall_multiplier=3):
        if any(type(value) is not int or value < 1 for value in (workers, pending, stall_multiplier)):
            raise ValueError('lane bounds must be positive integers')
        self.name = name
        self._queue = queue.Queue(maxsize=pending)
        self._closed = threading.Event()
        self._admission = threading.Lock()
        self._on_stalled = on_stalled
        self._stall_multiplier = stall_multiplier
        self._threads = [threading.Thread(target=self._work, name=f'voice-{name}-{i}', daemon=True)
                         for i in range(workers)]
        for thread in self._threads:
            thread.start()

    def submit(self, function, *args, timeout):
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError('work timeout must be positive and finite')
        future = Future()
        with self._admission:
            if self._closed.is_set():
                raise LaneBusy('voice lane is closing')
            try:
                self._queue.put_nowait((future, time.monotonic() + timeout, timeout, function, args))
            except queue.Full:
                raise LaneBusy('voice lane is at capacity') from None
        return future

    def _work(self):
        while not self._closed.is_set():
            try:
                job = self._queue.get(timeout=.1)
            except queue.Empty:
                continue
            try:
                self._execute(job)
            finally:
                self._queue.task_done()

    def _execute(self, job):
        future, deadline, timeout, function, args = job
        if self._closed.is_set():
            future.cancel()
            return
        if not future.set_running_or_notify_cancel():
            return
        if time.monotonic() >= deadline:
            future.set_exception(DeadlineExpired('queued voice request expired'))
            return
        completed = False
        completion_lock = threading.Lock()

        def stalled():
            with completion_lock:
                if not completed:
                    self._on_stalled(self.name)

        timer = threading.Timer(timeout * self._stall_multiplier, stalled)
        timer.daemon = True
        timer.start()
        try:
            result = function(*args)
        except BaseException as exc:
            error = exc
        else:
            error = None
        finally:
            with completion_lock:
                completed = True
            timer.cancel()
        # User callbacks on a Future may block; they are not native inference.
        if error is not None:
            future.set_exception(error)
        else:
            future.set_result(result)

    def close(self, timeout=2):
        with self._admission:
            self._closed.set()
            while True:
                try:
                    future, *_ = self._queue.get_nowait()
                except queue.Empty:
                    break
                future.cancel()
                self._queue.task_done()
        deadline = time.monotonic() + timeout
        for thread in self._threads:
            thread.join(max(0, deadline - time.monotonic()))
        return all(not thread.is_alive() for thread in self._threads)
