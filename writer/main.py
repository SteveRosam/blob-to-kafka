"""
Blob CSV writer (sink).

Consumes row records from a Kafka topic and writes them back to blob storage as
CSV files. Writes are batched: rows accumulate and are flushed as a single CSV
object per chunk, because blob writes are slow and one-write-per-message would be
far too chatty.

Chunking is driven by two limits (whichever comes first):
  CHUNK_MAX_ROWS          flush after this many rows       (commit_every)
  CHUNK_INTERVAL_SECONDS  flush at least this often         (commit_interval)

Each chunk is written to:
  <OUTPUT_PREFIX>/<topic>-p<partition>-<start_offset>-<end_offset>.csv

Configuration (env vars — see app.yaml / quix.yaml):
  input                   Kafka topic of row records to consume
  OUTPUT_PREFIX           Blob prefix to write CSV chunks under (default "processed")
  CHUNK_MAX_ROWS          Max rows per CSV chunk (default 1000)
  CHUNK_INTERVAL_SECONDS  Max seconds before a partial chunk is flushed (default 30)
  BLOB_BASE_PATH          Root path in blob storage (consumed by blob.py)
"""

import csv
import io
import logging
import os
from datetime import datetime, timezone

import blob
from quixstreams import Application
from quixstreams.sinks.base import BatchingSink, SinkBatch

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("blob-writer")

INPUT_TOPIC = os.environ.get("input", "processed-rows")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "blob-writer")
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "processed")
CHUNK_MAX_ROWS = int(os.environ.get("CHUNK_MAX_ROWS", "1000"))
CHUNK_INTERVAL_SECONDS = float(os.environ.get("CHUNK_INTERVAL_SECONDS", "30"))


class BlobCSVSink(BatchingSink):
    """Writes each committed batch of dict records to blob storage as one CSV object."""

    def write(self, batch: SinkBatch) -> None:
        rows = [item.value for item in batch if isinstance(item.value, dict)]
        if not rows:
            return

        # Column set = union of all row keys, in first-seen order.
        fieldnames: list[str] = []
        for row in rows:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
        payload = buf.getvalue().encode("utf-8")

        try:
            suffix = f"{batch.start_offset}-{batch.end_offset}"
        except AttributeError:
            suffix = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
        key = f"{OUTPUT_PREFIX}/{batch.topic}-p{batch.partition}-{suffix}.csv"

        try:
            blob.write_bytes(key, payload)
        except Exception:
            # Re-raise so the batch is retried from the last commit rather than lost.
            logger.exception("Failed to write chunk %s — batch will be retried", key)
            raise

        logger.info("Wrote %d rows (%d bytes) to %s", len(rows), len(payload), key)


def main() -> None:
    app = Application(
        consumer_group=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        commit_interval=CHUNK_INTERVAL_SECONDS,  # time-based chunk flush
        commit_every=CHUNK_MAX_ROWS,             # size-based chunk flush
    )
    input_topic = app.topic(INPUT_TOPIC, value_deserializer="json")

    logger.info(
        "Consuming %r → blob prefix %r (chunk: max %d rows / %.0fs)",
        input_topic.name, OUTPUT_PREFIX, CHUNK_MAX_ROWS, CHUNK_INTERVAL_SECONDS,
    )

    sdf = app.dataframe(input_topic)
    sdf.sink(BlobCSVSink())

    app.run()


if __name__ == "__main__":
    main()
