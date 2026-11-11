"""
In-flight capacity limiting for the Spider.

When the spider parses downloaded documents slower than the network
layer downloads them, the downloaded bodies pile up in internal
queues without any bound. The in-flight limiter puts a cap on the
number of task attempts that are being processed at the same time,
where "being processed" means the whole life-time of one task
attempt:

* the slot is acquired right before the attempt tries to read the
  document from the cache or from the network,
* the slot is held while the document is downloaded, dispatched,
  parsed (the task handler output is fully consumed) and written
  to the cache,
* failed attempts release the slot immediately (a retry is a new
  attempt and acquires a new slot),
* a cache miss that falls through to the network does NOT acquire
  a second slot: the slot travels with the task.

The limiter is disabled by default. It is enabled by passing a
positive integer as the ``in_flight_limit`` option of the Spider.
"""
import logging
from threading import Condition, Lock

# pylint: disable=invalid-name
logger = logging.getLogger("grab.spider.in_flight")
# pylint: enable=invalid-name


class InFlightLimiter(object):
    """
    A shutdown-aware counting semaphore that hands out slot tokens.

    The number of tokens given out and not yet released never
    exceeds ``limit``. After ``shutdown()`` has been called all
    blocked and future ``acquire()`` calls return ``None`` instead
    of a token, so that threads waiting for a free slot can wake up
    and exit.
    """

    def __init__(self, limit):
        self._limit = limit
        self._in_flight = 0
        self._shutdown = False
        self._cond = Condition()

    @property
    def limit(self):
        return self._limit

    @property
    def in_flight(self):
        """Number of slots currently given out."""
        with self._cond:
            return self._in_flight

    @property
    def is_shutdown(self):
        with self._cond:
            return self._shutdown

    def acquire(self, block=True):
        """
        Take one slot.

        Returns an ``InFlightToken`` or ``None`` if the limiter has
        been shut down or (with ``block=False``) no slot is free.
        """
        with self._cond:
            while not self._shutdown and self._in_flight >= self._limit:
                if not block:
                    return None
                self._cond.wait()
            if self._shutdown:
                return None
            self._in_flight += 1
            return InFlightToken(self)

    def release(self):
        with self._cond:
            self._in_flight -= 1
            self._cond.notify()

    def shutdown(self):
        """
        Wake up all threads waiting in ``acquire()`` and make all
        future acquisitions fail.
        """
        with self._cond:
            self._shutdown = True
            self._cond.notify_all()


class InFlightToken(object):
    """
    Represents one in-flight slot owned by one task attempt.

    The slot is released back to the limiter exactly once, when
    either:

    * all registered processing stages have completed (the parse
      stage is registered by default, an extra cache-write stage is
      registered with ``expect_cache_write()``), or
    * the attempt is aborted with ``abort()`` (failed attempts,
      dropped tasks, spider shutdown).
    """

    def __init__(self, limiter):
        self._limiter = limiter
        self._lock = Lock()
        # The parse stage is always expected
        self._pending = 1
        self._released = False

    def expect_cache_write(self):
        """
        Register an extra stage: the slot is released only after
        the cache write of this attempt has finished too.
        """
        with self._lock:
            if not self._released:
                self._pending += 1

    def complete(self):
        """Mark one registered stage as finished."""
        with self._lock:
            if self._released:
                return
            self._pending -= 1
            if self._pending <= 0:
                self._released = True
                self._limiter.release()

    def abort(self):
        """Release the slot immediately, regardless of pending stages."""
        with self._lock:
            if not self._released:
                self._released = True
                self._limiter.release()
