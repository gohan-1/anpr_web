"""
A simple rate limiter shared by all threads.

WHY: running 16 images at once could send 16+ requests per second to the plate API.
Most paid APIs have a limit (e.g. 10 per second) and reject or bill extra above it.
This makes every thread wait its turn so we never go over the limit.
"""
import threading
import time


class RateLimiter:
    def __init__(self, per_second: float):
        self.interval = 1.0 / per_second if per_second else 0.0
        self._lock = threading.Lock()
        self._next_slot = time.monotonic()

    def wait(self) -> None:
        if not self.interval:
            return
        with self._lock:                        # only one thread books a slot at a time
            now = time.monotonic()
            slot = max(now, self._next_slot)
            self._next_slot = slot + self.interval
        if slot > now:
            time.sleep(slot - now)              # sleep OUTSIDE the lock so others can book
