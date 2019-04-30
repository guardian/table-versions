package org.apache.hadoop.fs.versioned

import java.net.URI
import java.util.Objects

import com.gu.tableversions.metastore.VersionPaths
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

class VersionedFileSystem extends FileSystem {

  private var baseURI: URI = _
  private var baseFS: FileSystem = _
  private var mappings: Map[String, String] = _
  private var version: String = _

  override def initialize(name: URI, conf: Configuration): Unit = {
    val cacheDisabled = conf.getBoolean("fs.versioned.impl.disable.cache", false)
    val baseFSScheme = conf.get("fs.versioned.baseFS")
    version = conf.get("fs.versioned.version")

    require(Objects.nonNull(baseFSScheme), "fs.versioned.baseFS not set in configuration")
    require(Objects.nonNull(version), "fs.versioned.version not set in configuration")
    require(cacheDisabled, "fs.versioned.impl.disable.cache not set to true in configuration")

    import scala.collection.JavaConverters._

    mappings = conf.iterator.asScala
      .filter(_.getKey.startsWith(VersionPaths.KEY_PREFIX))
      .map(_.getValue)
      .toList
      .map { v =>
        (v, v + "/" + version)
      }
      .toMap

    // When initialising the versioned filesystem, we need to extract the
    // URI for the base filesystem, which is accessible under the scheme
    // specific part of the URI.
    baseURI = new URI(baseFSScheme, null, name.getSchemeSpecificPart, null, null)
    baseFS = FileSystem.get(baseURI, conf)
    baseFS.initialize(baseURI, conf)
  }

  override def getUri: URI = {
    new URI(s"${VersionedFileSystem.SCHEME}:${baseURI.getSchemeSpecificPart}")
  }

  override def open(f: Path, bufferSize: Int): FSDataInputStream =
    baseFS.open(baseVersionedPath(f), bufferSize)

  override def create(
      f: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream =
    baseFS.create(baseVersionedPath(f), permission, overwrite, bufferSize, replication, blockSize, progress)

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream =
    baseFS.append(baseVersionedPath(f), bufferSize, progress)

  override def rename(src: Path, dst: Path): Boolean =
    baseFS.rename(baseVersionedPath(src), baseVersionedPath(dst))

  override def delete(f: Path, recursive: Boolean): Boolean =
    baseFS.delete(baseVersionedPath(f), recursive)

  override def listStatus(f: Path): Array[FileStatus] =
    baseFS.listStatus(baseVersionedPath(f)) map { fileStatus =>
      new FileStatus(
        fileStatus.getLen,
        fileStatus.isDirectory,
        fileStatus.getReplication.toInt,
        fileStatus.getBlockSize,
        fileStatus.getModificationTime,
        versionedPath(fileStatus.getPath)
      )
    } filter { fileStatus =>
      fileStatus.getPath.toUri.getSchemeSpecificPart.contains(version) ||
      fileStatus.isDirectory
    }

  override def setWorkingDirectory(new_dir: Path): Unit =
    baseFS.setWorkingDirectory(baseVersionedPath(new_dir))

  override def getWorkingDirectory: Path = baseVersionedPath(baseFS.getWorkingDirectory)

  override def mkdirs(f: Path, permission: FsPermission): Boolean =
    baseFS.mkdirs(baseVersionedPath(f), permission)

  override def getFileStatus(f: Path): FileStatus = {
    val fileStatus = baseFS.getFileStatus(baseVersionedPath(f))

    new FileStatus(
      fileStatus.getLen,
      fileStatus.isDirectory,
      fileStatus.getReplication.toInt,
      fileStatus.getBlockSize,
      fileStatus.getModificationTime,
      versionedPath(fileStatus.getPath)
    )
  }

  private lazy val appendVersion: Path => Path = path => {
    val uri = path.toUri

    val sp = mappings.foldLeft(uri.getSchemeSpecificPart) {
      case (acc, (partition, versionedPartition)) if !acc.contains(versionedPartition) && acc.contains(partition) =>
        acc.replace(partition, versionedPartition)
      case (acc, _) => acc
    }

    new Path(new URI(s"${uri.getScheme}:$sp"))
  }

  private lazy val setScheme: String => Path => Path =
    scheme => path => new Path(new URI(s"$scheme:${path.toUri.getSchemeSpecificPart}"))

  private lazy val toSchemeAndVersion: String => Path => Path = scheme => setScheme(scheme) andThen appendVersion

  private lazy val baseVersionedPath: Path => Path = path => toSchemeAndVersion(baseURI.getScheme)(path)
  private lazy val versionedPath: Path => Path = path => setScheme(VersionedFileSystem.SCHEME)(path)
}

object VersionedFileSystem {
  val SCHEME = "versioned"
}
