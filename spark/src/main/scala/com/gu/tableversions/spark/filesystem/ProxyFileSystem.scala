package com.gu.tableversions.spark.filesystem

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs._
import org.apache.hadoop.util.Progressable

// A ProxyFileSystem wraps the behaviour of some other ("underlying" or "base") FileSystem,
// enabling transformations of `Path`s provided to the base `FileSystem` through the use of the
// `PathMapper`. Operations on the `ProxyFileSystem` ultimately delegate to the base filesystem,
// but first translate the paths provided to the `ProxyFileSystem` into a format suitable for
// that base filesystem.
//
// `Path` values returned by calls to the base filesystem are translated in the opposite
// direction by the `PathMapper`, making them suitable for use in future calls to the ProxyFileSystem.
abstract class ProxyFileSystem extends FileSystem {

  // The underlying FileSystem we delegate to. Paths in calls to this FileSystem
  // should be translated with the PathMapper.
  @volatile protected var baseFS: FileSystem = _

  // The root URI for the underlying FileSystem
  @volatile protected var baseURI: URI = _

  // A utility to map between Paths for the proxy and underlying FileSystems.
  @volatile protected var pathMapper: PathMapper = _

  def initialiseProxyFileSystem(_baseURI: URI, _pathMapper: PathMapper, hadoopConfig: Configuration): Unit = {
    baseURI = _baseURI
    pathMapper = _pathMapper

    baseFS = FileSystem.get(baseURI, hadoopConfig)
    baseFS.initialize(baseURI, hadoopConfig)
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

  override def listStatus(f: Path): Array[FileStatus] =
    baseFS.listStatus(pathMapper.forUnderlying(f)).map(status => mapFileStatusPath(status, pathMapper.fromUnderlying))

  override def setWorkingDirectory(new_dir: Path): Unit =
    baseFS.setWorkingDirectory(pathMapper.forUnderlying(new_dir))

  override def getWorkingDirectory: Path =
    pathMapper.fromUnderlying(baseFS.getWorkingDirectory)

  override def mkdirs(f: Path, permission: FsPermission): Boolean =
    baseFS.mkdirs(pathMapper.forUnderlying(f), permission)

  override def getFileStatus(f: Path): FileStatus =
    mapFileStatusPath(baseFS.getFileStatus(pathMapper.forUnderlying(f)), pathMapper.fromUnderlying)

  override def setReplication(src: Path, replication: Short): Boolean =
    baseFS.setReplication(pathMapper.forUnderlying(src), replication)

  override def setTimes(p: Path, mtime: Long, atime: Long): Unit =
    baseFS.setTimes(pathMapper.forUnderlying(p), mtime, atime)

  override def setOwner(f: Path, username: String, groupname: String): Unit =
    baseFS.setOwner(pathMapper.forUnderlying(f), username, groupname)

  override def getFileChecksum(f: Path): FileChecksum =
    baseFS.getFileChecksum(pathMapper.forUnderlying(f))

  // Apply a function to the path in the provided FileStatus
  private def mapFileStatusPath(fileStatus: FileStatus, f: Path => Path): FileStatus =
    new FileStatus(
      fileStatus.getLen,
      fileStatus.isDirectory,
      fileStatus.getReplication.toInt,
      fileStatus.getBlockSize,
      fileStatus.getModificationTime,
      f(fileStatus.getPath)
    )
}
