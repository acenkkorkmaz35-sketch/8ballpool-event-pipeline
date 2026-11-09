"""The Kafka connector must be baked into the image: no ``spark.jars.packages`` at runtime."""


def test_kafka_source_resolves_without_network(spark):
    assert spark.conf.get("spark.jars.packages", None) is None
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "clean-events")
        .option("kafka.isolation.level", "read_committed")
        .load()
    )
    assert df.isStreaming
    assert {"key", "value", "topic", "partition", "offset", "timestamp"} <= set(df.columns)
