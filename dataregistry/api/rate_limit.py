"""In-process rate limiting for unauthenticated lookups.

FailedLookupLimiter counts only *failed* lookups per client, so a visitor who
follows valid links is never throttled while someone guessing ids is cut off
after a few misses.

Clients are keyed on the direct peer address (request.client.host). Do not read
X-Forwarded-For: uvicorn only honours forwarded headers from 127.0.0.1, and there
is no reverse proxy in front of this API, so any such header comes straight from
the caller and would let them pick their own key.

State lives in this process's memory: counters are per process and reset on
restart. That is enough for a single uvicorn process; revisit (shared store,
trusted proxy headers) if a proxy or multiple workers are ever introduced.
"""
import math
import threading
import time
from collections import OrderedDict, deque


class FailedLookupLimiter:
    """Sliding-window limit on failed lookups per client.

    A client is blocked once it has `max_failures` failures recorded inside the last
    `window_seconds`. Callers check `retry_after` before doing the lookup and call
    `record_failure` only when the lookup found nothing.

    At most `max_tracked_clients` clients are remembered; beyond that the least
    recently seen is forgotten, so memory stays bounded however many addresses probe
    the endpoint.
    """

    def __init__(self, max_failures: int = 20, window_seconds: float = 60.0,
                 max_tracked_clients: int = 10_000, clock=time.monotonic):
        self._max_failures = max_failures
        self._window = window_seconds
        self._max_clients = max_tracked_clients
        self._clock = clock
        self._lock = threading.Lock()
        # client -> failure timestamps, oldest first; ordered least to most recently seen
        self._clients: 'OrderedDict[str, deque]' = OrderedDict()

    def retry_after(self, client: str) -> int:
        """0 if the client may proceed, else whole seconds (>= 1) until a slot frees.

        Records nothing, so polling while blocked cannot extend the block.
        """
        with self._lock:
            now = self._clock()
            failures = self._live_failures(client, now)
            if failures is None or len(failures) < self._max_failures:
                return 0
            return max(1, math.ceil(failures[0] + self._window - now))

    def record_failure(self, client: str) -> None:
        """Note a failed lookup by `client`.

        A client that is already blocked is not recorded again: its block ends when
        its oldest failure ages out, and requests that raced past the check must not
        push that back.
        """
        with self._lock:
            now = self._clock()
            failures = self._live_failures(client, now)
            if failures is None:
                failures = self._clients[client] = deque()
                while len(self._clients) > self._max_clients:
                    self._clients.popitem(last=False)
            if len(failures) < self._max_failures:
                failures.append(now)

    def reset(self) -> None:
        """Forget every client."""
        with self._lock:
            self._clients.clear()

    def _live_failures(self, client: str, now: float):
        """The client's unexpired failures, pruned in place and marked most recently
        seen; None (and the client dropped) if it has none. Caller holds the lock."""
        failures = self._clients.get(client)
        if failures is None:
            return None
        while failures and failures[0] + self._window <= now:
            failures.popleft()
        if not failures:
            del self._clients[client]
            return None
        self._clients.move_to_end(client)
        return failures
