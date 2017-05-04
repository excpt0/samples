import asyncio
import math

from common.log import applog


class PeriodicCallback():
    """Schedules the given asyncio coroutine to be called periodically.

    The callback is called every ``interval`` seconds (int or float).
    Note that the timeout is given in seconds.

    If the coroutine runs for longer than ``interval`` seconds,
    subsequent invocations will be skipped to get back on schedule.

    `start` must be called after the `PeriodicCallback` is created.
    """
    def __init__(self, callback, interval, io_loop=None):
        self.callback = callback
        if interval <= 0:
            raise ValueError("Periodic callback must have a positive interval")
        self.interval = interval
        self.io_loop = io_loop or asyncio.get_event_loop()
        self._running = False
        self._timeout = None

    def start(self):
        """Starts the timer."""
        applog.debug("start %r", self.callback)
        self._running = True
        self._next_timeout = self.io_loop.time()
        self._schedule_next()

    def stop(self):
        """Stops the timer."""
        self._running = False
        applog.debug("stop %r", self.callback)
        if self._timeout is not None:
            self._timeout.cancel()
            self._timeout = None

    def is_running(self):
        return self._running

    def _done_handler(self, f):
        """Done callback for asyncio task which helps to catch exceptions.

        :param f: Future object
        """
        try:
            exc = f.exception()
        except (asyncio.InvalidStateError, asyncio.CancelledError):
            pass
        else:
            if exc:
                applog.error(exc, exc_info=True)

    def _run(self):
        if not self._running:
            return
        try:
            r = self.callback()
            if asyncio.iscoroutine(r):
                task = self.io_loop.create_task(r)
                task.add_done_callback(self._done_handler)
        except Exception as e:
            applog.error("Exception in callback %r", self.callback, exc_info=True)
        finally:
            self._schedule_next()

    def _schedule_next(self):
        if self.is_running:
            current_time =  self.io_loop.time()

            if self._next_timeout <= current_time:
                coro_time_sec = self.interval
                self._next_timeout += (math.floor((current_time - self._next_timeout) /
                                                  coro_time_sec) + 1) * coro_time_sec

            self._timeout = self.io_loop.call_at(self._next_timeout, self._run)
