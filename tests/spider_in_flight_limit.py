"""
Tests for the Spider `in_flight_limit` option: a cap on the number
of task attempts being processed at the same time (downloaded but
not yet parsed/cached documents).

All tests use only a local HTTP test server and an in-memory
stub cache backend, no external services are required.
"""
import threading
import time
from unittest import TestCase

from test_server import Response

from grab.cookie import CookieManager
from grab.document import Document
from grab.spider import Spider, Task
from grab.spider.cache_service import CacheReaderService, CacheWriterService
from grab.spider.error import SpiderMisuseError
from tests.util import GLOBAL, BaseGrabTestCase, build_spider

# Allow running this module with plain pytest, without runtest.py
if GLOBAL["grab_transport"] is None:
    GLOBAL["grab_transport"] = "pycurl"
if GLOBAL["network_service"] is None:
    GLOBAL["network_service"] = "multicurl"

RUN_TIMEOUT = 30
NETWORK_SERVICES = ("multicurl", "threaded")


def run_bot(bot, timeout=RUN_TIMEOUT):
    """
    Run the spider in a separate thread, so that a deadlock in the
    in-flight limit logic fails the test instead of hanging the
    whole test suite.
    """
    errors = []

    def target():
        try:
            bot.run()
        except Exception as ex:  # pylint: disable=broad-except
            errors.append(ex)

    thread = threading.Thread(target=target)
    thread.daemon = True
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        raise AssertionError("Spider has not finished in %d seconds" % timeout)
    if errors:
        raise errors[0]


class Control(object):
    """Thread-safe observation point shared with the spider via `meta`."""

    def __init__(self, parse_sleep=0):
        self.parse_sleep = parse_sleep
        self.lock = threading.Lock()
        self.events = []
        self.parsed = []
        self.current = 0
        self.max_concurrent = 0

    def log(self, *args):
        with self.lock:
            self.events.append(args)


class StubCacheBackend(object):
    """
    In-memory cache backend with the same interface as the mongodb
    backend. Supports artificial slow writes and event logging.
    """

    def __init__(self, write_sleep=0, log=None):
        self.storage = {}
        self.write_sleep = write_sleep
        self.log = log if log is not None else (lambda *args: None)

    def get_item(self, url):
        return self.storage.get(url)

    def load_response(self, grab, cache_item):
        grab.setup_document(cache_item["body"])

        def custom_prepare_response_func(transport, grab):
            doc = Document()
            doc.head = cache_item["head"]
            doc.body = cache_item["body"]
            doc.code = cache_item["response_code"]
            doc.download_size = len(cache_item["body"])
            doc.upload_size = 0
            doc.download_speed = 0
            doc.url = cache_item["response_url"]
            doc.parse(charset=grab.config["document_charset"])
            doc.cookies = CookieManager(transport.extract_cookiejar())
            doc.from_cache = True
            return doc

        grab.process_request_result(custom_prepare_response_func)

    def save_response(self, url, grab):
        self.log("write_start", url)
        if self.write_sleep:
            time.sleep(self.write_sleep)
        self.storage[url] = {
            "body": grab.doc.body,
            "head": grab.doc.head,
            "response_code": grab.doc.code,
            "response_url": grab.doc.url,
        }
        self.log("write_end", url)

    def size(self):
        return len(self.storage)

    def close(self):
        pass


def attach_cache(bot, backend):
    bot.cache_reader_service = CacheReaderService(bot, backend)
    bot.cache_writer_service = CacheWriterService(bot, backend)


class PageSpider(Spider):
    def task_page(self, grab, task):
        ctl = self.meta["control"]
        ctl.log("parse_start", task.url)
        if ctl.parse_sleep:
            time.sleep(ctl.parse_sleep)
        ctl.log("parse_end", task.url)
        ctl.parsed.append((task.url, grab.doc.body))


class ConcurrencySpider(Spider):
    def task_page(self, grab, task):
        ctl = self.meta["control"]
        with ctl.lock:
            ctl.current += 1
            ctl.max_concurrent = max(ctl.max_concurrent, ctl.current)
        time.sleep(ctl.parse_sleep)
        with ctl.lock:
            ctl.current -= 1
            ctl.parsed.append(task.url)


class SubtaskSpider(Spider):
    def task_page(self, grab, task):
        for idx in range(3):
            yield Task("item", url=self.meta["server"].get_url("/item/%d" % idx))

    def task_item(self, grab, task):
        self.meta["control"].parsed.append(task.url)


class InFlightLimitConfigTestCase(TestCase):
    def build(self, **kwargs):
        return build_spider(Spider, **kwargs)

    def test_limit_disabled_by_default(self):
        bot = self.build()
        self.assertIsNone(bot.in_flight_limiter)

    def test_valid_limit(self):
        bot = self.build(in_flight_limit=3)
        self.assertEqual(3, bot.in_flight_limiter.limit)

    def test_limit_from_config_dict(self):
        bot = self.build(config={"in_flight_limit": 2})
        self.assertEqual(2, bot.in_flight_limiter.limit)

    def test_invalid_limit_values(self):
        for value in (0, -1, 1.5, "2", True):
            self.assertRaises(SpiderMisuseError, self.build, in_flight_limit=value)

    def test_invalid_limit_value_in_config_dict(self):
        self.assertRaises(
            SpiderMisuseError, self.build, config={"in_flight_limit": 0}
        )


class InFlightLimitTestCase(BaseGrabTestCase):
    def build_bot(self, spider_cls, control, network_service, **kwargs):
        kwargs.setdefault("parser_pool_size", 4)
        bot = build_spider(
            spider_cls,
            meta={"control": control, "server": self.server},
            network_service=network_service,
            **kwargs
        )
        bot.setup_queue()
        return bot

    # ******************************
    # Slow parsing: concurrency cap
    # ******************************

    def check_concurrency_bound(self, network_service):
        self.server.add_response(Response(data=b"ok"), count=-1)
        ctl = Control(parse_sleep=0.2)
        bot = self.build_bot(
            ConcurrencySpider,
            ctl,
            network_service,
            thread_number=4,
            in_flight_limit=2,
        )
        for _ in range(6):
            bot.add_task(Task("page", url=self.server.get_url()))
        run_bot(bot)
        self.assertEqual(6, len(ctl.parsed))
        # The limit is respected (<= 2) and the spider is not
        # forcibly serialized (== 2, not 1)
        self.assertEqual(2, ctl.max_concurrent)
        self.assertEqual(0, bot.in_flight_limiter.in_flight)
        if network_service == "multicurl":
            # While all slots were busy the new attempts were
            # postponed, not blocked inside the result-dispatching
            # thread
            self.assertTrue(
                bot.stat.counters["spider:in-flight-deferred"] >= 1
            )

    def test_concurrency_bound_multicurl(self):
        self.check_concurrency_bound("multicurl")

    def test_concurrency_bound_threaded(self):
        self.check_concurrency_bound("threaded")

    # ***************************************************
    # Slow cache write: slot is held till the write ends
    # ***************************************************

    def check_slot_held_till_cache_write(self, network_service):
        self.server.add_response(Response(data=b"ok"), count=-1)
        ctl = Control(parse_sleep=0.1)
        backend = StubCacheBackend(write_sleep=0.4, log=ctl.log)
        bot = self.build_bot(
            PageSpider, ctl, network_service, in_flight_limit=1
        )
        attach_cache(bot, backend)
        url1 = self.server.get_url("/one")
        url2 = self.server.get_url("/two")
        bot.add_task(Task("page", url=url1))
        bot.add_task(Task("page", url=url2))
        run_bot(bot)
        self.assertEqual(2, len(ctl.parsed))
        self.assertEqual(2, backend.size())
        # With in_flight_limit=1 the second attempt can not start
        # before the parsing AND the cache write of the first
        # attempt have both finished
        first_url = [x[1] for x in ctl.events if x[0] == "parse_start"][0]
        second_url = url2 if first_url == url1 else url1
        idx_write_end = ctl.events.index(("write_end", first_url))
        idx_parse_start2 = ctl.events.index(("parse_start", second_url))
        self.assertTrue(idx_parse_start2 > idx_write_end)
        self.assertEqual(0, bot.in_flight_limiter.in_flight)

    def test_slot_held_till_cache_write_multicurl(self):
        self.check_slot_held_till_cache_write("multicurl")

    def test_slot_held_till_cache_write_threaded(self):
        self.check_slot_held_till_cache_write("threaded")

    # *************************************************
    # Cache hit and cache miss (no double slot acquire)
    # *************************************************

    def check_cache_hit_and_miss(self, network_service):
        self.server.add_response(Response(data=b"from-network"), count=-1)
        backend = StubCacheBackend()
        url_a = self.server.get_url("/a")
        url_b = self.server.get_url("/b")

        # Phase 1: cache miss -> network -> cache write
        ctl1 = Control()
        bot1 = self.build_bot(
            PageSpider, ctl1, network_service, in_flight_limit=1
        )
        attach_cache(bot1, backend)
        bot1.add_task(Task("page", url=url_a))
        run_bot(bot1)
        self.assertEqual(1, backend.size())
        self.assertEqual(1, bot1.stat.counters["cache:req-miss"])
        self.assertEqual(0, bot1.in_flight_limiter.in_flight)

        # Phase 2: cache hit for url_a, cache miss -> network for
        # url_b. With in_flight_limit=1 any double slot acquisition
        # on the miss->network path would deadlock the spider.
        ctl2 = Control()
        bot2 = self.build_bot(
            PageSpider, ctl2, network_service, in_flight_limit=1
        )
        attach_cache(bot2, backend)
        bot2.add_task(Task("page", url=url_a))
        bot2.add_task(Task("page", url=url_b))
        run_bot(bot2)
        self.assertEqual(1, bot2.stat.counters["cache:req-hit"])
        self.assertEqual(1, bot2.stat.counters["cache:req-miss"])
        bodies = dict(ctl2.parsed)
        self.assertEqual(b"from-network", bodies[url_a])
        self.assertEqual(b"from-network", bodies[url_b])
        self.assertEqual(2, backend.size())
        self.assertEqual(0, bot2.in_flight_limiter.in_flight)

    def test_cache_hit_and_miss_multicurl(self):
        self.check_cache_hit_and_miss("multicurl")

    def test_cache_hit_and_miss_threaded(self):
        self.check_cache_hit_and_miss("threaded")

    # **************************************************
    # Failed attempt releases the slot, retry re-enters
    # **************************************************

    def check_failed_attempt_retry(self, network_service):
        self.server.add_response(Response(status=500))
        self.server.add_response(Response(data=b"recovered"))
        ctl = Control()
        bot = self.build_bot(
            PageSpider, ctl, network_service, in_flight_limit=1
        )
        url = self.server.get_url()
        bot.add_task(Task("page", url=url))
        run_bot(bot)
        self.assertEqual(2, bot.stat.counters["spider:request-network"])
        self.assertEqual([(url, b"recovered")], ctl.parsed)
        self.assertEqual(0, bot.in_flight_limiter.in_flight)

    def test_failed_attempt_retry_multicurl(self):
        self.check_failed_attempt_retry("multicurl")

    def test_failed_attempt_retry_threaded(self):
        self.check_failed_attempt_retry("threaded")

    # **************************************
    # Tasks yielded from the task handler
    # **************************************

    def check_subtasks(self, network_service):
        self.server.add_response(Response(data=b"ok"), count=-1)
        ctl = Control()
        bot = self.build_bot(
            SubtaskSpider, ctl, network_service, in_flight_limit=1
        )
        bot.add_task(Task("page", url=self.server.get_url("/main")))
        run_bot(bot)
        self.assertEqual(3, len(ctl.parsed))
        self.assertEqual(0, bot.in_flight_limiter.in_flight)

    def test_subtasks_multicurl(self):
        self.check_subtasks("multicurl")

    def test_subtasks_threaded(self):
        self.check_subtasks("threaded")

    # ************
    # only_cache
    # ************

    def check_only_cache(self, network_service):
        self.server.add_response(Response(data=b"cached"), count=-1)
        backend = StubCacheBackend()
        url_a = self.server.get_url("/a")
        url_b = self.server.get_url("/b")

        # Populate the cache with url_a
        ctl1 = Control()
        bot1 = self.build_bot(
            PageSpider, ctl1, network_service, in_flight_limit=1
        )
        attach_cache(bot1, backend)
        bot1.add_task(Task("page", url=url_a))
        run_bot(bot1)
        self.assertEqual(1, backend.size())

        # only_cache spider: url_a is served from the cache,
        # url_b misses the cache and is dropped. Both must not
        # deadlock the spider.
        ctl2 = Control()
        bot2 = self.build_bot(
            PageSpider,
            ctl2,
            network_service,
            in_flight_limit=1,
            only_cache=True,
        )
        attach_cache(bot2, backend)
        bot2.add_task(Task("page", url=url_a))
        bot2.add_task(Task("page", url=url_b))
        run_bot(bot2)
        parsed_urls = [url for url, _ in ctl2.parsed]
        self.assertIn(url_a, parsed_urls)
        self.assertNotIn(url_b, parsed_urls)
        self.assertEqual(0, bot2.in_flight_limiter.in_flight)

    def test_only_cache_multicurl(self):
        self.check_only_cache("multicurl")

    def test_only_cache_threaded(self):
        self.check_only_cache("threaded")

    # **********************************
    # Stop wakes up slot waiters
    # **********************************

    def check_stop_wakes_slot_waiters(self, network_service):
        self.server.add_response(Response(data=b"ok"), count=-1)
        ctl = Control(parse_sleep=1.0)
        bot = self.build_bot(
            PageSpider,
            ctl,
            network_service,
            thread_number=3,
            in_flight_limit=1,
        )
        for _ in range(5):
            bot.add_task(Task("page", url=self.server.get_url()))
        thread = threading.Thread(target=bot.run)
        thread.daemon = True
        thread.start()
        # Wait until the first task is being parsed, i.e. the only
        # in-flight slot is taken and other network workers are
        # waiting for it
        deadline = time.time() + 10
        while time.time() < deadline:
            if any(x[0] == "parse_start" for x in ctl.events):
                break
            time.sleep(0.05)
        else:
            self.fail("Parsing has not started")
        bot.stop()
        start = time.time()
        thread.join(20)
        self.assertFalse(thread.is_alive(), "Spider has not stopped")
        self.assertTrue(bot.in_flight_limiter.is_shutdown)
        # Stopping must take seconds (the ongoing slow parse), not
        # tens of seconds (waiting for stuck slot waiters)
        self.assertLess(time.time() - start, 15)

    def test_stop_wakes_slot_waiters_multicurl(self):
        self.check_stop_wakes_slot_waiters("multicurl")

    def test_stop_wakes_slot_waiters_threaded(self):
        self.check_stop_wakes_slot_waiters("threaded")
