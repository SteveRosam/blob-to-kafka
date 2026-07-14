# blob processor

A small Quix Cloud pipeline that demonstrates loading files from blob storage in
response to Kafka messages.

```
Generator ──(blob-files topic)──▶ Blob Processor ──▶ loads file from blob storage
  publishes                          consumes msg,
  { "file_name": ... }               reads file_name, loads bytes, processes
```

## Services

### `generator/`
Publishes file-reference messages to the `blob-files` topic on an interval, cycling
through a configurable list of blob keys. Exists purely to give the processor
something to do. Publishes messages like:

```json
{ "file_name": "samples/readings_1.csv", "seq": 0 }
```

### `processor/`
Consumes those messages, reads the referenced object from blob storage via
[`processor/blob.py`](processor/blob.py), and processes the bytes. The processing
step (run a model, parse CSV, …) is a commented-out placeholder in
`process_message`.

`blob.py` is a generic, provider-agnostic client built on `quixportal` — it works
over AWS S3, Azure, GCP, MinIO, S3-compatible, or Local storage with no code change
(the provider comes from the `Quix__BlobStorage__Connection__Json` config Quix
injects when `blobStorage: bind: true`).

## Sample data

`sample_data/` holds three CSVs (sensor readings) referenced by the generator's
default `FILE_NAMES`.

**Upload them so the keys match what the generator publishes.** The generator emits
`file_name = "samples/readings_1.csv"`, and the processor resolves that relative to
`BLOB_BASE_PATH`. So upload each file to:

```
<BLOB_BASE_PATH>/samples/readings_1.csv
<BLOB_BASE_PATH>/samples/readings_2.csv
<BLOB_BASE_PATH>/samples/readings_3.csv
```

i.e. drop the contents of `sample_data/` into a `samples/` folder under your blob
storage root. (With `BLOB_BASE_PATH` empty, that's just `samples/…`.)

## Layout

```
quix.yaml              # pipeline descriptor (both deployments + topic)
sample_data/           # CSVs to upload into blob storage
generator/
  app.yaml
  dockerfile
  requirements.txt
  main.py              # publishes file-reference messages
processor/
  app.yaml
  dockerfile
  requirements.txt
  main.py              # consumes messages, loads + processes files
  blob.py              # generic quixportal blob client
```

## Configuration

**Generator**

| Variable | Default | Purpose |
|---|---|---|
| `output` | `blob-files` | Topic to publish to (matches processor's `input`) |
| `FILE_NAMES` | `samples/readings_1.csv,…` | Blob keys to cycle through |
| `PUBLISH_INTERVAL_SECONDS` | `10` | Seconds between messages |

**Processor**

| Variable | Default | Purpose |
|---|---|---|
| `input` | `blob-files` | Topic to consume from |
| `FILE_NAME_FIELD` | `file_name` | Message field holding the blob key |
| `BLOB_BASE_PATH` | `` | Root path in blob storage |

## Deploy

Commit and push to the branch the target Quix environment watches; Quix picks up
`quix.yaml` and offers to deploy both services. The processor has
`blobStorage: bind: true`, so storage credentials are injected automatically.
Upload the sample CSVs (see above) before starting the generator.
