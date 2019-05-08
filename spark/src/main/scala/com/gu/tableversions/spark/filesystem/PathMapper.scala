package com.gu.tableversions.spark.filesystem

import java.net.URI

import com.gu.tableversions.core.{Partition, Version}
import org.apache.hadoop.fs.Path

/**
  * Map paths between one "outer" FileSystem (e.g. a ProxyFileSystem) and an underlying one.
  * For example, by changing the schema, or appending version directories.
  */
trait PathMapper {

  /** Convert a path with a "versioned" scheme to one suitable for the underlying FileSystem. */
  def forUnderlying(path: Path): Path

  /** Convert a path from the underlying FileSystem to one in the "versioned" scheme. */
  def fromUnderlying(path: Path): Path
}

/**
  * Path mapper that converts paths for partitions according to a pre-configured set of versioned partitions.
  */
class VersionedPathMapper(underlyingFsScheme: String, partitionVersions: Map[Partition, Version]) extends PathMapper {

  import VersionedPathMapper._

  override def forUnderlying(path: Path): Path = {
    assert(path.toUri.getScheme == VersionedFileSystem.scheme,
           s"Path provided to `forUnderlying` ($path) not in the ${VersionedFileSystem.scheme} scheme")

    appendVersion(setUnderlyingScheme(path))
  }

  override def fromUnderlying(path: Path): Path = {
    assert(path.toUri.getScheme == underlyingFsScheme,
           s"Path provided to `fromUnderlying` ($path) not in the underlying $underlyingFsScheme scheme")

    setVersionedScheme(stripVersion(path))
  }

  private def stripVersion(path: Path): Path = {
    partitionVersions
      .find {
        case (partition, _) =>
          path.toString.contains(VersionedPathMapper.normalize(partition.toString))
      }
      .map {
        case (_, version) =>
          new Path(path.toString.replace(s"/${version.label}", ""))
      }
      .getOrElse(path)
  }

  private def appendVersion(path: Path): Path = {
    val uri = path.toUri
    val uriSchemeSpecificPart = uri.getSchemeSpecificPart

    val versionForPath = partitionVersions
      .find {
        case (partition, version) =>
          uriSchemeSpecificPart.contains(normalize(partition.toString)) && !uriSchemeSpecificPart
            .contains(version.label)
      }

    versionForPath
      .map {
        case (partition, version) =>
          new Path(
            path.toString.replace(normalize(partition.toString), s"${normalize(partition.toString)}/${version.label}"))
      }
      .getOrElse(path)
  }

  private def setUnderlyingScheme(path: Path): Path =
    setScheme(underlyingFsScheme, path)

}

object VersionedPathMapper {

  private def normalize(partition: String): String =
    if (partition.startsWith("/")) partition else s"/$partition"

  private def setScheme(scheme: String, path: Path): Path =
    new Path(new URI(s"$scheme:${path.toUri.getSchemeSpecificPart}"))

  private def setVersionedScheme(path: Path): Path =
    setScheme(VersionedFileSystem.scheme, path)

}
