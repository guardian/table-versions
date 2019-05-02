package com.gu.tableversions.spark

import java.net.URI

import org.apache.hadoop.fs.Path

// Map paths between one "outer" FileSystem and an underlying one
// e.g. by changing the schema or appending version directories
trait PathMapper {

  def forUnderlying(path: Path): Path

  def fromUnderlying(path: Path): Path
}

class VersionedPathMapper(underlyingFsScheme: String, partitionMappings: Map[String, String]) extends PathMapper {

  // convert a path with a "versioned" scheme to one suitable for the underlying FileSystem:
  // - replace the versioned schema with the base FS
  // - append the version directory to the path
  override def forUnderlying(path: Path): Path = {
    assert(path.toUri.getScheme == VersionedFileSystem.scheme,
           s"Path provided to `forUnderlying` ($path) not in the ${VersionedFileSystem.scheme} scheme")

    appendVersion(setUnderlyingScheme(path))
  }

  // convert a path from the underlying FileSystem to one in the "versioned" scheme:
  // - set the schema to "versioned"
  // - remove the version directory from the path (if it exists)
  override def fromUnderlying(path: Path): Path = {
    assert(path.toUri.getScheme == underlyingFsScheme,
           s"Path provided to `fromUnderlying` ($path) not in the underlying $underlyingFsScheme scheme")

    val pathWithoutVersionDirectory = partitionMappings
      .find {
        case (_, versionedPartition) =>
          path.toString.contains(versionedPartition)
      }
      .map {
        case (partition, versionedPartition) =>
          new Path(path.toString.replace(versionedPartition, partition))
      }
      .getOrElse(setVersionedScheme(path))

    setVersionedScheme(pathWithoutVersionDirectory)
  }

  private def appendVersion(path: Path): Path = {
    val uri = path.toUri

    partitionMappings
      .find {
        case (partition, versionedPartition) =>
          uri.getSchemeSpecificPart.contains(partition) && !uri.getSchemeSpecificPart.contains(versionedPartition)
      }
      .map {
        case (partition, versionedPartition) =>
          new Path(path.toString.replace(normalize(partition), normalize(versionedPartition)))
      }
      .getOrElse(path)
  }

  private def normalize(partition: String): String =
    if (partition.startsWith("/")) partition else s"/$partition"

  private def setScheme(scheme: String, path: Path): Path =
    new Path(new URI(s"$scheme:${path.toUri.getSchemeSpecificPart}"))

  private def setUnderlyingScheme(path: Path): Path =
    setScheme(underlyingFsScheme, path)

  private def setVersionedScheme(path: Path): Path =
    setScheme(VersionedFileSystem.scheme, path)

}
