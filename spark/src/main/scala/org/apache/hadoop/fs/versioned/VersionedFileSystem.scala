package org.apache.hadoop.fs.versioned

import java.net.URI
import java.util.Objects

import com.gu.tableversions.core.Partition
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

class VersionedFileSystem extends FileSystem {

  private var baseURI: URI = _
  private var baseFS: FileSystem = _
  private var partitionMappings: Map[String, String] = _
  private var version: String = _

  override def initialize(name: URI, conf: Configuration): Unit = {
    val cacheDisabled = conf.getBoolean("fs.versioned.impl.disable.cache", false)
    val baseFSScheme = conf.get("fs.versioned.baseFS")
    version = conf.get("fs.versioned.version")

    require(Objects.nonNull(baseFSScheme), "fs.versioned.baseFS not set in configuration")
    require(Objects.nonNull(version), "fs.versioned.version not set in configuration")
    require(cacheDisabled, "fs.versioned.impl.disable.cache not set to true in configuration")

    import scala.collection.JavaConverters._

    partitionMappings = conf.iterator.asScala
      .filter(_.getKey.startsWith(VersionedFileSystem.KEY_PREFIX))
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

  override def getUri: URI =
    new URI(s"${VersionedFileSystem.SCHEME}:${baseURI.getSchemeSpecificPart}")

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

  private def toggleFileStatus(fileStatus: FileStatus, isParam: Boolean): FileStatus = {
    new FileStatus(
      fileStatus.getLen,
      fileStatus.isDirectory,
      fileStatus.getReplication.toInt,
      fileStatus.getBlockSize,
      fileStatus.getModificationTime,
      if (isParam) baseVersionedPath(fileStatus.getPath) else versionedPath(fileStatus.getPath)
    )
  }

  override def listStatus(f: Path): Array[FileStatus] =
    baseFS.listStatus(baseVersionedPath(f)) map { fileStatus =>
      toggleFileStatus(fileStatus, false)
    } filter { fileStatus =>
      fileStatus.getPath.toUri.getSchemeSpecificPart.contains(version) ||
      fileStatus.isDirectory
    }

  override def setWorkingDirectory(new_dir: Path): Unit =
    baseFS.setWorkingDirectory(baseVersionedPath(new_dir))

  override def getWorkingDirectory: Path = baseVersionedPath(baseFS.getWorkingDirectory)

  override def mkdirs(f: Path, permission: FsPermission): Boolean =
    baseFS.mkdirs(baseVersionedPath(f), permission)

  override def getFileStatus(f: Path): FileStatus =
    toggleFileStatus(baseFS.getFileStatus(baseVersionedPath(f)), false)

//  override def getFileBlockLocations(file: FileStatus, start: Long, len: Long): Array[BlockLocation] =
//    super.getFileBlockLocations(toggleFileStatus(file, true), start, len)
//
//  override def makeQualified(f: Path): Path = baseFS.makeQualified(baseVersionedPath(f))
//
//  override def checkPath(f: Path): Unit = super.checkPath(baseVersionedPath(f))
//
//  override def setReplication(f: Path, replication: Short): Boolean =
//    baseFS.setReplication(baseVersionedPath(f), replication)
//
//  override def copyFromLocalFile(src: Path, dst: Path): Unit =
//    baseFS.copyFromLocalFile(baseVersionedPath(src), baseVersionedPath(dst))
//
//  override def copyFromLocalFile(delSrc: Boolean, src: Path, dst: Path): Unit =
//    baseFS.copyFromLocalFile(delSrc, baseVersionedPath(src), baseVersionedPath(dst))
//
//  override def copyFromLocalFile(delSrc: Boolean, overwrite: Boolean, srcs: Array[Path], dst: Path): Unit =
//    baseFS.copyFromLocalFile(delSrc, overwrite, srcs, baseVersionedPath(dst))
//
//  override def copyFromLocalFile(delSrc: Boolean, overwrite: Boolean, src: Path, dst: Path): Unit =
//    baseFS.copyFromLocalFile(delSrc, overwrite, src, baseVersionedPath(dst))
//
//  override def copyToLocalFile(src: Path, dst: Path): Unit = baseFS.copyToLocalFile(src, dst)
//
//  override def deleteOnExit(f: Path): Boolean = baseFS.deleteOnExit(baseVersionedPath(f))
//
//  override def getHomeDirectory: Path = versionedPath(baseFS.getHomeDirectory)
//
//  override def startLocalOutput(fsOutputFile: Path, tmpLocalFile: Path): Path =
//    baseFS.startLocalOutput(fsOutputFile, tmpLocalFile)
//
//  override def getFileChecksum(f: Path): FileChecksum = super.getFileChecksum(baseVersionedPath(f))
//
//  override def setOwner(f: Path, username: String, groupname: String): Unit =
//    super.setOwner(baseVersionedPath(f), username, groupname)
//
//  override def setTimes(f: Path, mtime: Long, atime: Long): Unit = super.setTimes(baseVersionedPath(f), mtime, atime)
//
//  override def setPermission(f: Path, permission: FsPermission): Unit =
//    super.setPermission(baseVersionedPath(f), permission)

  def appendVersion(path: Path): Path = {
    val uri = path.toUri

    val specificPart = partitionMappings.foldLeft(uri.getSchemeSpecificPart) {
      case (acc, (partition, versionedPartition)) if !acc.contains(versionedPartition) && acc.contains(partition) =>
        acc.replace(partition, versionedPartition)
      case (acc, _) => acc
    }

    new Path(new URI(s"${uri.getScheme}:$specificPart"))
  }

  def setScheme(scheme: String, path: Path): Path =
    new Path(new URI(s"$scheme:${path.toUri.getSchemeSpecificPart}"))

  def baseVersionedPath(path: Path): Path = appendVersion(setScheme(baseURI.getScheme, path))

  def versionedPath(path: Path): Path = setScheme(VersionedFileSystem.SCHEME, path)
}

object VersionedFileSystem {
  val SCHEME = "versioned"
  val KEY_PREFIX = s"${SCHEME}__"

  def partitionMappings(partitions: List[Partition]): Map[String, String] = {
    partitions.map { partition =>
      val columnValueHivePath = partition.columnValues.map(cv => cv.column.name + "=" + cv.value).toList.mkString("/")
      (KEY_PREFIX + columnValueHivePath, columnValueHivePath)
    }.toMap
  }
}
