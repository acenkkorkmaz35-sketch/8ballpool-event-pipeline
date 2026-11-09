"""Values shared by more than one part of the pipeline (DQ service and Spark jobs)."""

# Label for a country or platform that cannot be determined (user without a known profile).
UNKNOWN = "UNKNOWN"

# One log line format for every process (generator, DQ service, Spark jobs).
LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
