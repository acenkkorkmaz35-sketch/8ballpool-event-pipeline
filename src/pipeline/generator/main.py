"""Event generator process: feeds simulated 8 Ball Pool events into ``raw-events``.

Wraps :class:`pipeline.generator.simulator.UserSimulator` in a paced Kafka producer loop.
The producer is idempotent (``enable.idempotence``), so a retried send can never create a
duplicate in the log. Messages are keyed by user id so one user's events stay ordered.
"""

import logging
import signal
import sys
import threading
import time

from confluent_kafka import KafkaError, Message, Producer

from pipeline.constants import LOG_FORMAT
from pipeline.generator.simulator import UserSimulator
from pipeline.settings import Settings

log = logging.getLogger("pipeline.generator")

REPORT_EVERY_S = 10.0


def _on_delivery(err: KafkaError | None, msg: Message) -> None:
    """Delivery callback: log failed sends (successes are counted by the main loop)."""
    if err is not None:
        log.error("delivery failed for key %s: %s", msg.key(), err)


def run(settings: Settings) -> None:
    """Produce events at ``settings.events_per_second`` until SIGTERM/SIGINT, then flush."""
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())

    producer = Producer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "enable.idempotence": True,
        }
    )
    simulator = UserSimulator(seed=settings.seed, invalid_rate=settings.invalid_rate, max_users=settings.max_users)
    interval = 1.0 / max(settings.events_per_second, 1)
    log.info("producing ~%d events/s to %s (seed=%d)", settings.events_per_second, settings.raw_topic, settings.seed)

    sent = 0
    sent_at_last_report = 0
    last_report = next_due = time.monotonic()
    try:
        while not stop.is_set():
            key, value = simulator.next_event(int(time.time()))
            try:
                producer.produce(settings.raw_topic, key=key.encode("utf-8"), value=value, on_delivery=_on_delivery)
            except BufferError:
                producer.poll(1.0)  # local queue full: let it drain, then drop this event
                continue
            producer.poll(0)  # serve delivery callbacks
            sent += 1

            now = time.monotonic()
            if now - last_report >= REPORT_EVERY_S:
                log.info("sent %d events (%.0f/s)", sent, (sent - sent_at_last_report) / (now - last_report))
                sent_at_last_report, last_report = sent, now

            # Schedule-based pacing: sleeping until a fixed deadline avoids drifting
            # below the target rate when produce() itself takes time.
            next_due += interval
            delay = next_due - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            else:
                next_due = time.monotonic()  # fell behind: do not try to catch up in a burst
    finally:
        log.info("shutting down, flushing (%d events sent)", sent)
        producer.flush(30)


def main() -> None:
    """Console entry point (``pipeline-generator``)."""
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    try:
        run(Settings.from_env())
    except Exception:
        log.exception("generator terminated by an unrecoverable error")
        sys.exit(1)


if __name__ == "__main__":
    main()
