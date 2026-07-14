# blob processor

A Quix Cloud pipeline that reads files from blob storage in response to Kafka
messages, parses them, and writes the results back to blob storage as CSV.

```
Generator ──▶ blob-files ──▶ Processor ──▶ processed-rows ──▶ Writer ──▶ blob storage
 publishes                    loads file       row records      batches rows,
 { file_name }                from blob,                        writes CSV chunks
                              parses CSV,                        under processed/
                              emits rows
```

## Services

### `generator/`
Publishes file-reference messages to `blob-files` on an interval, cycling through a
configurable list of blob keys. Test driver only.
```json
{ "file_name": "readings_1.csv", "seq": 0 }
```

### `processor/`
Consumes those messages, loads the referenced object from blob storage via
[`processor/blob.py`](processor/blob.py), parses it as CSV, and emits one record per
row to `processed-rows` (each row tagged with `source_file`).

### `writer/`
Consumes `processed-rows` and writes them back to blob storage as CSV. Writes are
**batched** — rows accumulate and flush as one CSV object per chunk (blob writes are
slow, so one write per message would be far too chatty). A chunk flushes when either
limit is hit:

| Limit | Default | Env var |
|---|---|---|
| rows per chunk | 1000 | `CHUNK_MAX_ROWS` |
| seconds per chunk | 30 | `CHUNK_INTERVAL_SECONDS` |

Chunks land at `<OUTPUT_PREFIX>/<topic>-p<partition>-<start_offset>-<end_offset>.csv`
(default prefix `processed/`). Naming by offset range makes re-processing idempotent.

`blob.py` is a generic, provider-agnostic client built on `quixportal` — it works
over AWS S3, Azure, GCP, MinIO, S3-compatible, or Local storage with no code change.
(It's duplicated in `processor/` and `writer/` because Quix builds each app from its
own folder — a build can't `COPY` across folders.)

## Sample data

`sample_data/` holds three CSVs (sensor readings) referenced by the generator's
default `FILE_NAMES`. Upload them so the keys match what the generator publishes —
the generator emits bare names like `readings_1.csv`, resolved relative to
`BLOB_BASE_PATH`:

```
<BLOB_BASE_PATH>/readings_1.csv
<BLOB_BASE_PATH>/readings_2.csv
<BLOB_BASE_PATH>/readings_3.csv
```

With `BLOB_BASE_PATH` empty, just drop them at the storage root. The writer's output
lands under `processed/`, a separate prefix, so it never collides with the inputs.

## Layout

```
quix.yaml              # pipeline descriptor (3 deployments + 2 topics)
sample_data/           # CSVs to upload into blob storage
generator/             # publishes file-reference messages
processor/             # loads + parses files, emits rows   (blob.py)
writer/                # batches rows, writes CSV chunks      (blob.py)
```

## Configuration

**Generator** — `output` (topic), `FILE_NAMES`, `PUBLISH_INTERVAL_SECONDS`
**Processor** — `input`, `output`, `FILE_NAME_FIELD`, `BLOB_BASE_PATH`
**Writer** — `input`, `OUTPUT_PREFIX`, `CHUNK_MAX_ROWS`, `CHUNK_INTERVAL_SECONDS`, `BLOB_BASE_PATH`

## Deploy

Commit and push to the branch the target Quix environment watches; Quix picks up
`quix.yaml` and offers to deploy all three services. Processor and writer have
`blobStorage: bind: true`, so storage credentials are injected automatically. Upload
the sample CSVs (see above) before starting the generator.
