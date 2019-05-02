package com.gu.tableversions.spark

import java.net.URI
import java.util.Objects

import com.gu.tableversions.core.Partition
import org.apache.hadoop.conf.Configuration

class VersionedFileSystem extends ProxyFileSystem {

  override def initialize(name: URI, conf: Configuration): Unit = {
    val cacheDisabled = conf.getBoolean("fs.versioned.impl.disable.cache", false)
    val baseFsScheme = conf.get("fs.versioned.baseFS")
    val version = conf.get("fs.versioned.version")

    require(Objects.nonNull(baseFsScheme), "fs.versioned.baseFS not set in configuration")
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

    // When initialising the versioned filesystem, we need to extract the
    // URI for the base filesystem, which is accessible under the scheme
    // specific part of the URI.
    val baseURI = new URI(baseFsScheme, null, name.getSchemeSpecificPart, null, null)
    val pathMapper = new VersionedPathMapper(baseFsScheme, partitionMappings)

    initialiseProxyFileSystem(baseURI, pathMapper, conf)
  }
}

object VersionedFileSystem {

  val scheme = "versioned"
  val keyPrefix = "fs.versioned.partitionMapping."

  def partitionMappings(partitions: List[Partition]): Map[String, String] = {
    partitions.map { partition =>
      val columnValueHivePath = partition.columnValues.map(cv => cv.column.name + "=" + cv.value).toList.mkString("/")
      (keyPrefix + columnValueHivePath, columnValueHivePath)
    }.toMap
  }
}
