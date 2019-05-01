package com.gu.tableversions.spark

import java.net.URI
import java.util.Objects

import com.gu.tableversions.core.Partition
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

abstract class ProxyFileSystem extends FileSystem {

  var pathMapper: PathMapper = _

  def setPathMapper(mapper: PathMapper): Unit =
    pathMapper = mapper
}

class VersionedFileSystem extends ProxyFileSystem {

  private var baseURI: URI = _
  private var baseFS: FileSystem = _
  private var version: String = _

  override def initialize(name: URI, conf: Configuration): Unit = {
    val cacheDisabled = conf.getBoolean("fs.versioned.impl.disable.cache", false)
    val baseFSScheme = conf.get("fs.versioned.baseFS")
    version = conf.get("fs.versioned.version")

    require(Objects.nonNull(baseFSScheme), "fs.versioned.baseFS not set in configuration")
    require(Objects.nonNull(version), "fs.versioned.version not set in configuration")
    require(cacheDisabled, "fs.versioned.impl.disable.cache not set to true in configuration")

    import scala.collection.JavaConverters._

    val partitionMappings = conf.iterator.asScala
      .filter(_.getKey.startsWith(VersionedFileSystem.keyPrefix))
      .map(_.getValue)
      .toList
      .map { v =>
        (v, v + "/" + version)
      }
      .toMap

    setPathMapper(new VersionedPathMapper(baseFSScheme, partitionMappings))

    // When initialising the versioned filesystem, we need to extract the
    // URI for the base filesystem, which is accessible under the scheme
    // specific part of the URI.
    baseURI = new URI(baseFSScheme, null, name.getSchemeSpecificPart, null, null)
    baseFS = FileSystem.get(baseURI, conf)
    baseFS.initialize(baseURI, conf)
  }

  override def getUri: URI =
    new URI(s"${VersionedFileSystem.scheme}:${baseURI.getSchemeSpecificPart}")

  override def open(f: Path, bufferSize: Int): FSDataInputStream =
    baseFS.open(pathMapper.forUnderlying(f), bufferSize)

  override def create(
      f: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream =
    baseFS.create(pathMapper.forUnderlying(f), permission, overwrite, bufferSize, replication, blockSize, progress)

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream =
    baseFS.append(pathMapper.forUnderlying(f), bufferSize, progress)

  override def rename(src: Path, dst: Path): Boolean =
    baseFS.rename(pathMapper.forUnderlying(src), pathMapper.forUnderlying(dst))

  override def delete(f: Path, recursive: Boolean): Boolean =
    baseFS.delete(pathMapper.forUnderlying(f), recursive)

  private def toggleFileStatus(fileStatus: FileStatus, isParam: Boolean): FileStatus = {
    new FileStatus(
      fileStatus.getLen,
      fileStatus.isDirectory,
      fileStatus.getReplication.toInt,
      fileStatus.getBlockSize,
      fileStatus.getModificationTime,
      if (isParam) pathMapper.forUnderlying(fileStatus.getPath) else pathMapper.fromUnderlying(fileStatus.getPath)
    )
  }

  override def listStatus(f: Path): Array[FileStatus] =
    baseFS.listStatus(pathMapper.forUnderlying(f)) map { fileStatus =>
      toggleFileStatus(fileStatus, false)
    } filter { fileStatus =>
      fileStatus.getPath.toUri.getSchemeSpecificPart.contains(version) ||
      fileStatus.isDirectory
    }

  override def setWorkingDirectory(new_dir: Path): Unit =
    baseFS.setWorkingDirectory(pathMapper.forUnderlying(new_dir))

  override def getWorkingDirectory: Path =
    pathMapper.fromUnderlying(baseFS.getWorkingDirectory)

  override def mkdirs(f: Path, permission: FsPermission): Boolean =
    baseFS.mkdirs(pathMapper.forUnderlying(f), permission)

  override def getFileStatus(f: Path): FileStatus =
    toggleFileStatus(baseFS.getFileStatus(pathMapper.forUnderlying(f)), false)

//  override def getFileBlockLocations(file: FileStatus, start: Long, len: Long): Array[BlockLocation] =
//    super.getFileBlockLocations(toggleFileStatus(file, true), start, len)
//
//  override def makeQualified(f: Path): Path = baseFS.makeQualified(pathModifier.forUnderlying(f))
//
//  override def checkPath(f: Path): Unit = super.checkPath(pathModifier.forUnderlying(f))
//
//  override def setReplication(f: Path, replication: Short): Boolean =
//    baseFS.setReplication(pathModifier.forUnderlying(f), replication)
//
//  override def copyFromLocalFile(src: Path, dst: Path): Unit =
//    baseFS.copyFromLocalFile(pathModifier.forUnderlying(src), pathModifier.forUnderlying(dst))
//
//  override def copyFromLocalFile(delSrc: Boolean, src: Path, dst: Path): Unit =
//    baseFS.copyFromLocalFile(delSrc, pathModifier.forUnderlying(src), pathModifier.forUnderlying(dst))
//
//  override def copyFromLocalFile(delSrc: Boolean, overwrite: Boolean, srcs: Array[Path], dst: Path): Unit =
//    baseFS.copyFromLocalFile(delSrc, overwrite, srcs, pathModifier.forUnderlying(dst))
//
//  override def copyFromLocalFile(delSrc: Boolean, overwrite: Boolean, src: Path, dst: Path): Unit =
//    baseFS.copyFromLocalFile(delSrc, overwrite, src, pathModifier.forUnderlying(dst))
//
//  override def copyToLocalFile(src: Path, dst: Path): Unit = baseFS.copyToLocalFile(src, dst)
//
//  override def deleteOnExit(f: Path): Boolean = baseFS.deleteOnExit(pathModifier.forUnderlying(f))
//
//  override def getHomeDirectory: Path = versionedPath(baseFS.getHomeDirectory)
//
//  override def startLocalOutput(fsOutputFile: Path, tmpLocalFile: Path): Path =
//    baseFS.startLocalOutput(fsOutputFile, tmpLocalFile)
//
//  override def getFileChecksum(f: Path): FileChecksum = super.getFileChecksum(pathModifier.forUnderlying(f))
//
//  override def setOwner(f: Path, username: String, groupname: String): Unit =
//    super.setOwner(pathModifier.forUnderlying(f), username, groupname)
//
//  override def setTimes(f: Path, mtime: Long, atime: Long): Unit = super.setTimes(pathModifier.forUnderlying(f), mtime, atime)
//
//  override def setPermission(f: Path, permission: FsPermission): Unit =
//    super.setPermission(pathModifier.forUnderlying(f), permission)
}

object VersionedFileSystem {
  val scheme = "versioned"
  val keyPrefix = s"${scheme}__"

  def partitionMappings(partitions: List[Partition]): Map[String, String] = {
    partitions.map { partition =>
      val columnValueHivePath = partition.columnValues.map(cv => cv.column.name + "=" + cv.value).toList.mkString("/")
      (keyPrefix + columnValueHivePath, columnValueHivePath)
    }.toMap
  }
}
