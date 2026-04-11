# 06 — Storage and Merge I/O

Cloud storage in IndexTables4Spark is deliberately *not* routed through the Hadoop `FileSystem` API for the hot paths. Instead, a lightweight `CloudStorageProvider` abstraction talks directly to the cloud SDKs (AWS SDK v2, Azure SDK) for fast parallel and async operations. Merges add a second layer on top: an async download/upload pipeline that can stream hundreds of GB of splits through a single executor without exhausting the heap.

This chapter covers `spark/io/` and `spark/io/merge/`.

## Cloud storage providers

### `io/CloudStorageProvider.scala`
The trait every provider implements. Defines upload, download, list, head, delete, and multipart operations on a cloud URI. Also defines `CloudFileInfo` (path, size, last modified). This is the contract that the read/write paths rely on — they never touch an AWS/Azure SDK class directly.

### `io/S3CloudStorageProvider.scala`
AWS SDK v2 S3 implementation. Uses the async S3 client where it matters and delegates large uploads to `S3MultipartUploader`. Picks up credentials via `utils/CredentialProviderFactory` and `util/ConfigNormalization`, so a single set of `spark.indextables.aws.*` keys reach every call site.

### `io/S3MultipartUploader.scala`
Helper for multipart S3 uploads. Splits a local file into parts, uploads them in parallel, and retries individual parts on transient failure.

### `io/AzureCloudStorageProvider.scala`
Azure Blob Storage implementation. Supports multiple authentication strategies (account key, connection string, OAuth/AAD via DefaultAzureCredential). Talks to the Azure SDK directly.

### `io/HadoopCloudStorageProvider.scala`
Fallback implementation for local filesystems, HDFS, and any other Hadoop-compatible filesystem. Wraps `org.apache.hadoop.fs.FileSystem`. Used for unit tests and HDFS deployments; in cloud environments the S3/Azure providers are used instead for performance.

### `io/ProtocolBasedIOFactory.scala`
The dispatcher. Looks at the URI scheme (`s3://`, `s3a://`, `s3n://`, `azure://`, `wasb://`, `wasbs://`, `abfs://`, `abfss://`, `hdfs://`, `file://`) and returns the appropriate provider instance. Also defines the `StorageProtocol` sealed trait so other code can pattern-match on the detected protocol.

## Merge I/O pipeline

Merges can touch hundreds of splits. Downloading them sequentially would be too slow, and downloading them all at once could exhaust executor heap or disk. The merge I/O pipeline exists to solve both problems: bounded concurrency, priority batching, and direct-to-disk streaming.

### `io/merge/AsyncDownloader.scala`
Base trait. Defines `DownloadRequest` (source, destination, priority) and `DownloadResult` (success, failure, retry count). Every protocol-specific downloader implements it.

### `io/merge/S3AsyncDownloader.scala`
S3 implementation built on `S3AsyncClient` and `AsyncResponseTransformer.toFile()`. The key property is that the data never hits JVM heap — the async client streams from the socket directly into a file on disk.

### `io/merge/AzureAsyncDownloader.scala`
Azure equivalent, using `BlobServiceAsyncClient` for direct-to-disk streaming. Same heap-avoiding design.

### `io/merge/LocalCopyDownloader.scala`
"Downloader" for local filesystems and anything already mounted (NFS, FUSE). Just does a `Files.copy()`. Keeps the rest of the pipeline protocol-agnostic.

### `io/merge/CloudDownloadManager.scala`
The orchestrator. A JVM-wide singleton that:

- Holds a `Semaphore` bounding total concurrent downloads.
- Accepts batched download requests and hands them off to the right protocol-specific downloader.
- Retries with exponential backoff on transient errors.
- Supports cancellation so a failing merge can abort its in-flight downloads.
- Uses `PriorityDownloadQueue` to ensure batch ordering.

### `io/merge/PriorityDownloadQueue.scala`
Two-level priority queue. The first level is batch submission order (first-submitted batches finish first), the second level is order within a batch. This stops a second batch from starving the first even under high concurrency.

### `io/merge/MergeIOConfig.scala`
All tuning knobs for the pipeline: max concurrency per core, total memory budget, per-download retry count, upload concurrency, and exponential backoff parameters. Defaults are conservative for laptops and scale up on cluster hardware.

### `io/merge/MergeIOThreadPools.scala`
Manages the JVM-wide singleton thread pools: one for download coordination, one for uploads, one for retry scheduling. Threads are daemons so they never block JVM shutdown. Auto-recreates pools if they are torn down (useful in notebooks where Spark contexts come and go).

### `io/merge/MergeUploader.scala`
The upload side of the merge pipeline. Uploads a locally-merged split back to cloud storage, reusing the same credential resolution as the downloaders. Also used by the post-merge commit step to make the merged split visible before the transaction log commit.

## How the pieces connect

```
spark/core/*                    spark/merge/*              spark/sync/*
  (reads/writes)                (merge jobs)               (companion sync)
      │                              │                         │
      └──────────────┬───────────────┴───────────┬─────────────┘
                     ▼                           ▼
            ProtocolBasedIOFactory      CloudDownloadManager
                     │                           │
       ┌────────────┬┴┬────────────┐   ┌─────────┴──────────┐
       ▼            ▼ ▼            ▼   ▼                    ▼
    S3Cloud…    Azure…  Hadoop…    PriorityDownloadQueue   MergeUploader
    Provider                             │
                                  ┌──────┼──────────┬─────────────┐
                                  ▼      ▼          ▼             ▼
                          S3AsyncDownloader  Azure…  LocalCopy…   (…)
                                  │
                              AWS SDK v2 async
                              (direct-to-disk)
```

The next chapter covers the in-JVM subsystems that drive this pipeline: merge-on-write, purge-on-write, and prewarm.
