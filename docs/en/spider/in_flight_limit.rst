.. _spider_in_flight_limit:

In-flight Request Limit
=======================

By default the spider does not limit how many downloaded documents are
waiting to be parsed. If your task handlers parse documents slower than
the network service downloads them, the downloaded bodies pile up in the
internal queues and the memory usage of the spider grows without any
bound.

The ``in_flight_limit`` option puts a cap on the number of task attempts
being processed at the same time:

.. code:: python

    bot = MySpider(in_flight_limit=10)
    bot.run()

The option accepts a positive integer and is disabled (``None``) by
default. Any other value raises
``grab.spider.error.SpiderMisuseError`` when the spider instance is
created, i.e. before any spider service has started. You can also set
the limit with the ``in_flight_limit`` key of the spider config dict.

How it works
------------

One task attempt occupies one in-flight slot:

* the slot is acquired right before the attempt reads the document from
  the cache or from the network;
* the slot is held while the document is downloaded, dispatched and
  parsed, and is released only after the task handler output has been
  fully consumed *and* the cache write of that document has finished;
* if the attempt fails (network error, invalid HTTP status), the slot is
  released immediately; the retry is a new attempt and acquires a new
  slot;
* if the cache misses and the task falls through to the network, the
  slot travels with the task: the attempt never occupies two slots.

When all slots are busy, only the start of new attempts is postponed.
Dispatching of completed requests, parsing and cache writing keep
progressing, so the slots are eventually released and the postponed
attempts continue. Tasks yielded from task handlers, automatic retries
and the ``only_cache`` mode work as usual and can not deadlock the
spider.

The limit is applied per spider instance and works with both network
services (``multicurl`` and ``threaded``). When the spider stops, all
threads waiting for a free slot are woken up and finish immediately.
