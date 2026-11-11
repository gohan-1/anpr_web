import threading
import time

from pipeline.ratelimit import RateLimiter


def test_limits_calls_across_threads():
    limiter = RateLimiter(per_second=20)
    calls = []

    def worker():
        for _ in range(5):
            limiter.wait()
            calls.append(time.monotonic())

    threads = [threading.Thread(target=worker) for _ in range(4)]
    start = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # 20 calls at 20/second must take at least ~0.95 seconds
    assert len(calls) == 20
    assert time.monotonic() - start >= 0.9


def test_zero_means_no_limit():
    limiter = RateLimiter(per_second=0)
    start = time.monotonic()
    for _ in range(1000):
        limiter.wait()
    assert time.monotonic() - start < 0.1
