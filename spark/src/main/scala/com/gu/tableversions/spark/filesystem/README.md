# Versioned file system

This page describes some details about the "versioned file system" mechanism that we use to write versioned data.

## Usage

When using Spark and other tools in the Hadoop ecosystem, data is written via a `Filesystem` object that reads and writes to underlying storage.
Concrete implementations of this class exist for a wide variety of back-ends, for example HDFS, AWS S3, Google Cloud Storage, Azure Blob Storage, as well as local files.
We implement a proxy filesystem, `VersionedFilesystem`, which wraps the underlying file system that you would normally use, and which performs the translation of paths according to current versions for partitions in the data.

This means we can transparently use for example Spark Dataset methods for writing datasets, e.g.:

```scala
    dataset.write
      .partitionBy(partitions)
      .parquet(path)

```

...and data will be written to the right path for each partition.

To use this file system in a job, you need to configure this file system. You do this via the following Hadoop configuration properties:

```
fs.versioned.impl=com.gu.tableversions.spark.VersionedFileSystem
fs.versioned.impl.disable.cache=true
fs.versioned.baseFS=<the scheme of the underlying filesystem, e.g. "s3" or "hdfs"
fs.versioned.configDirectory=<the path where the partition version configuration will be written>
```

Note that to provide these configuration settings via Spark config, you have to prefix the config keys with "spark.hadoop.".
The method `VersionedFilesystem.sparkConfig` is a convenience method that will create the right configuration parameters for you.
