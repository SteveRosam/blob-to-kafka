"""
Kafka → blob file processor.

Consumes messages from a Kafka topic. Each message carries a `file_name` field
pointing at an object in blob storage. For every message the service loads that
file's bytes from blob storage and then processes it (run a model, parse CSV, …).

Message shape (JSON):
  { "file_name": "path/to/object.csv", ... }

Configuration (env vars — see app.yaml / quix.yaml):
  input           Kafka topic to consume from (Quix convention)
  FILE_NAME_FIELD Field in the message holding the blob key (default "file_name")
  BLOB_BASE_PATH  Root path in blob storage (consumed by blob.py)
"""

import logging
import os

import blob
from quixstreams import Application

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("blob-processor")

INPUT_TOPIC = os.environ.get("input", "blob-files")
FILE_NAME_FIELD = os.environ.get("FILE_NAME_FIELD", "file_name")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "blob-processor")


def process_message(value: dict):
    """Load the referenced file from blob storage and process it."""
    file_name = value.get(FILE_NAME_FIELD)
    if not file_name:
        logger.warning("Message missing %r field; skipping: %r", FILE_NAME_FIELD, value)
        return value

    data = blob.read_bytes(file_name, default=None)
    if data is None:
        logger.warning("File not found in blob storage: %s", file_name)
        return value

    logger.info("Loaded %s (%d bytes)", file_name, len(data))

    # ------------------------------------------------------------------ #
    # Do something with the file bytes. Pick whichever fits, e.g.:
    #
    # Run a model:
    #   result = model.predict(data)
    #   value["prediction"] = result
    #
    # Extract CSV rows:
    #   import io, csv
    #   rows = list(csv.DictReader(io.StringIO(data.decode("utf-8"))))
    #   value["row_count"] = len(rows)
    #
    # Load into a dataframe:
    #   import io, pandas as pd
    #   df = pd.read_csv(io.BytesIO(data))
    #   value["summary"] = df.describe().to_dict()
    # ------------------------------------------------------------------ #

    return value


def main() -> None:
    app = Application(consumer_group=CONSUMER_GROUP, auto_offset_reset="earliest")
    input_topic = app.topic(INPUT_TOPIC, value_deserializer="json")

    logger.info("Consuming from topic %r (group %r)", input_topic.name, CONSUMER_GROUP)

    sdf = app.dataframe(input_topic)
    sdf = sdf.update(process_message)

    # To forward results to another topic, declare an OutputTopic variable and:
    #   output_topic = app.topic(os.environ["output"], value_serializer="json")
    #   sdf = sdf.to_topic(output_topic)

    app.run()


if __name__ == "__main__":
    main()
