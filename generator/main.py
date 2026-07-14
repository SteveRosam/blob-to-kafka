"""
Test generator for the Blob Processor.

Publishes file-reference messages to a Kafka topic so the processor has something
to consume. Each message names a file the processor will load from blob storage:

  { "file_name": "samples/readings_1.csv", "seq": 0 }

Drop the matching CSV files (see the repo's sample_data/ folder) into your blob
storage under BLOB_BASE_PATH so the processor can find them.

Configuration (env vars — see app.yaml / quix.yaml):
  output                    Kafka topic to publish to (must match the processor's `input`)
  FILE_NAMES                Comma-separated blob keys to cycle through
  PUBLISH_INTERVAL_SECONDS  Seconds between messages (default 10)
"""

import json
import logging
import os
import signal
import time

from quixstreams import Application

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("generator")

OUTPUT_TOPIC = os.environ.get("output", "blob-files")
FILE_NAMES = [
    f.strip()
    for f in os.environ.get(
        "FILE_NAMES",
        "samples/readings_1.csv,samples/readings_2.csv,samples/readings_3.csv",
    ).split(",")
    if f.strip()
]
PUBLISH_INTERVAL_SECONDS = int(os.environ.get("PUBLISH_INTERVAL_SECONDS", "10"))

_running = True


def _stop(signum, _frame):
    global _running
    logger.info("Received signal %s — shutting down", signum)
    _running = False


def main() -> None:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    app = Application()
    topic = app.topic(OUTPUT_TOPIC, value_serializer="json")

    logger.info(
        "Publishing to %r every %ss, cycling %d file(s): %s",
        topic.name, PUBLISH_INTERVAL_SECONDS, len(FILE_NAMES), FILE_NAMES,
    )

    with app.get_producer() as producer:
        seq = 0
        while _running:
            file_name = FILE_NAMES[seq % len(FILE_NAMES)]
            message = {"file_name": file_name, "seq": seq}
            producer.produce(
                topic=topic.name,
                key=file_name.encode(),
                value=json.dumps(message).encode(),
            )
            logger.info("Published %r", message)
            seq += 1

            for _ in range(PUBLISH_INTERVAL_SECONDS):
                if not _running:
                    break
                time.sleep(1)

    logger.info("Stopped.")


if __name__ == "__main__":
    main()
