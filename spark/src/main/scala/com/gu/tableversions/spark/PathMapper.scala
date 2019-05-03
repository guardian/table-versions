package com.gu.tableversions.spark

import java.net.URI

import com.gu.tableversions.core.Version
import org.apache.hadoop.fs.Path

// Map paths between one "outer" FileSystem (e.g. a ProxyFileSystem) and an underlying one.
// For example, by changing the schema, or appending version directories.
trait PathMapper {

  def forUnderlying(path: Path): Path

  def fromUnderlying(path: Path): Path
}

class VersionedPathMapper(underlyingFsScheme: String, version: Version) extends PathMapper {

  // convert a path with a "versioned" scheme to one suitable for the underlying FileSystem:
  // - replace the versioned schema with the base FS
  // - append the version directory to the path
  override def forUnderlying(path: Path): Path = {
    assert(path.toUri.getScheme == VersionedFileSystem.scheme,
           s"Path provided to `forUnderlying` ($path) not in the ${VersionedFileSystem.scheme} scheme")

    // Split path into folders
    // Right to left:
    // Find thing that looks like partition
    // Insert version after this

    val parts = path.toUri.getSchemeSpecificPart.split("/")
    val (afterLastPartition, upToLastPartitionFolder) = parts.reverse.span(part => !isPartitionFolder(part))
    val convertedPath =
      if (upToLastPartitionFolder.isEmpty)
        path
      else
        new Path((afterLastPartition ++ Seq(version.label) ++ upToLastPartitionFolder).reverse.mkString("/"))

    val result = setScheme(underlyingFsScheme, convertedPath)
    // TODO: Remove this debug output
    println(s"${Thread.currentThread().getId}: ${callingMethod()} converted forUnderlying from $path to $result")
    result
  }

  def callingMethod(): String = {
    val stacktrace = Thread.currentThread.getStackTrace.toList
    val e = stacktrace(3)
    e.getMethodName
  }

  // convert a path from the underlying FileSystem to one in the "versioned" scheme:
  // - set the schema to "versioned"
  // - remove the version directory from the path (if it exists)
  override def fromUnderlying(path: Path): Path = {
    assert(path.toUri.getScheme == underlyingFsScheme,
           s"Path provided to `fromUnderlying` ($path) not in the underlying $underlyingFsScheme scheme")

    val parts = path.toUri.getSchemeSpecificPart.split("/")
    val (afterLastPartition, upToLastPartitionFolder) = parts.reverse.span(part => !isPartitionFolder(part))
    val convertedPath =
      if (upToLastPartitionFolder.isEmpty)
        path
      else new Path((afterLastPartition.dropRight(1) ++ upToLastPartitionFolder).reverse.mkString("/"))

    val result = setScheme(VersionedFileSystem.scheme, convertedPath)
    // TODO: Remove this debug output
    println(s"${Thread.currentThread().getId}: ${callingMethod()} converted fromUnderlying from $path to $result")
    result
  }

  private def isPartitionFolder(path: String): Boolean = path.matches("""[^\=]+\=[^\=]+""")

  private def setScheme(scheme: String, path: Path): Path =
    new Path(new URI(s"$scheme:${path.toUri.getSchemeSpecificPart}"))

}
