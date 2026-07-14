"""
Kafka → blob file processor.

Consumes messages from a Kafka topic. Each message carries a `file_name` field
pointing at an object in blob storage. For every message the service loads that
file's bytes from blob storage, parses it as CSV, and emits one record per row to
the output topic (for the writer to persist).

Input message shape (JSON):
  { "file_name": "readings_1.csv", ... }

Output record shape (JSON, one per CSV row):
  { "<csv columns...>": ..., "source_file": "readings_1.csv" }

Configuration (env vars — see app.yaml / quix.yaml):
  input           Kafka topic to consume from
  output          Kafka topic to emit parsed rows to
  FILE_NAME_FIELD Field in the message holding the blob key (default "file_name")
  BLOB_BASE_PATH  Root path in blob storage (consumed by blob.py)
"""

import csv
import io
import logging
import os

import blob
from quixstreams import Application

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("blob-processor")

INPUT_TOPIC = os.environ.get("input", "blob-files")
OUTPUT_TOPIC = os.environ.get("output", "processed-rows")
FILE_NAME_FIELD = os.environ.get("FILE_NAME_FIELD", "file_name")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "blob-processor")


def load_and_parse(value: dict) -> list[dict]:
    """Load the referenced file from blob storage and parse it into row records."""
    file_name = value.get(FILE_NAME_FIELD)
    if not file_name:
        logger.warning("Message missing %r field; skipping: %r", FILE_NAME_FIELD, value)
        return []

    data = blob.read_bytes(file_name, default=None)
    if data is None:
        logger.warning("File not found in blob storage: %s", file_name)
        return []

    try:
        rows = list(csv.DictReader(io.StringIO(data.decode("utf-8"))))
    except Exception:
        logger.exception("Failed to parse CSV: %s", file_name)
        return []

    for row in rows:
        row["source_file"] = file_name

    logger.info("Parsed %d rows from %s", len(rows), file_name)
    return rows

    # ---- Alternatives to CSV parsing -------------------------------------- #
    # Run a model:   return [{"file_name": file_name, "prediction": model.predict(data)}]
    # Dataframe:     df = pd.read_csv(io.BytesIO(data)); return df.to_dict("records")
    # ----------------------------------------------------------------------- #


def main() -> None:
    app = Application(consumer_group=CONSUMER_GROUP, auto_offset_reset="earliest")
    input_topic = app.topic(INPUT_TOPIC, value_deserializer="json")
    output_topic = app.topic(OUTPUT_TOPIC, value_serializer="json")

    logger.info("Consuming %r → emitting rows to %r", input_topic.name, output_topic.name)

    sdf = app.dataframe(input_topic)
    sdf = sdf.apply(load_and_parse, expand=True)  # expand: one output message per row
    sdf = sdf.to_topic(output_topic)

    app.run()


if __name__ == "__main__":
    main()
