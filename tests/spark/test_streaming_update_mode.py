"""Key assumption check: exact distinct users via size(collect_set) in streaming update mode."""

import json

from pipeline.spark.aggregations import minute_summary
from pipeline.spark.parsing import parse_clean_events, to_participants

MINUTE = 1700000040  # minute boundary (2023-11-14 22:14:00 UTC)


def _init(user: str, t: int) -> str:
    return json.dumps({"event-type": "init", "time": t, "user-id": user, "country": "TR", "platform": "ANDROID"})


def _run_once(spark, src, checkpoint, sink: list) -> None:
    """Process everything currently in ``src`` (one availableNow run) into ``sink``."""
    raw = spark.readStream.text(str(src))
    events = parse_clean_events(raw).withWatermark("event_time", "2 minutes")
    summary = minute_summary(to_participants(events))

    def collect(batch_df, batch_id: int) -> None:
        for row in batch_df.collect():
            sink.append((batch_id, row.window_start, row.purchases_count, row.distinct_users))

    query = (
        summary.writeStream.outputMode("update")
        .foreachBatch(collect)
        .option("checkpointLocation", str(checkpoint))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()


def test_streaming_update_mode_collect_set(spark, tmp_path):
    src, checkpoint = tmp_path / "src", tmp_path / "checkpoint"
    src.mkdir()
    sink: list = []

    # Run 1: two distinct users in minute M.
    (src / "batch1.txt").write_text("\n".join([_init("u1", MINUTE + 5), _init("u2", MINUTE + 10)]) + "\n")
    _run_once(spark, src, checkpoint, sink)
    assert [row[3] for row in sink] == [2]

    # Run 2 (same checkpoint): one new user and one repeat in the same, still open, minute.
    # The refined row for M must show 3 (exact, not 4 and not a delta).
    sink.clear()
    (src / "batch2.txt").write_text("\n".join([_init("u3", MINUTE + 20), _init("u1", MINUTE + 30)]) + "\n")
    _run_once(spark, src, checkpoint, sink)
    assert [row[3] for row in sink] == [3]
    assert sink[0][1].strftime("%Y-%m-%d %H:%M:%S") == "2023-11-14 22:14:00"


def test_out_of_order_event_refines_minute_and_too_late_event_is_dropped(spark, tmp_path):
    src, checkpoint = tmp_path / "src", tmp_path / "checkpoint"
    src.mkdir()
    sink: list = []
    next_file = iter(range(100))

    def run(*events: str) -> dict:
        """Write one file of events, process it, and return {window_start: distinct_users}."""
        sink.clear()
        (src / f"batch{next(next_file)}.txt").write_text("\n".join(events) + "\n")
        _run_once(spark, src, checkpoint, sink)
        return {row[1].strftime("%H:%M:%S"): row[3] for row in sink}

    m, next_m = "22:14:00", "22:20:00"  # window starts of MINUTE and of MINUTE + 400 s

    assert run(_init("u1", MINUTE + 5), _init("u2", MINUTE + 10)) == {m: 2}
    assert run(_init("u3", MINUTE + 30)) == {m: 3}  # max event time seen is now M+0:30

    # Out of order: M+0:02 is earlier than the max seen (M+0:30) but the watermark
    # (max - 2 min) is still before the end of minute M, so a NEW user refines minute M again.
    assert run(_init("u4", MINUTE + 2)) == {m: 4}

    # Event time jumps to M+6:40: the watermark moves to M+4:40, far past the end of minute M.
    assert run(_init("u5", MINUTE + 400)) == {next_m: 1}

    # A new user in minute M is now beyond the watermark: dropped as too late, M is untouched.
    # The open minute is refined in the same run, which proves the run did process the file.
    refined = run(_init("u6", MINUTE + 20), _init("u7", MINUTE + 410))
    assert refined == {next_m: 2}
    assert m not in refined
