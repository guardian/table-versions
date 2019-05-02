package com.gu.tableversions.spark

import java.net.URI
import java.util.Objects

import com.gu.tableversions.core.Partition
import com.gu.tableversions.spark.VersionedFileSystem.ConfigKeys
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

class VersionedFileSystem extends ProxyFileSystem {

  override def initialize(name: URI, conf: Configuration): Unit = {
    val cacheDisabled = conf.getBoolean(ConfigKeys.disableCache, false)
    val baseFsScheme = conf.get(ConfigKeys.baseFS)
    val version = conf.get(ConfigKeys.version)

    require(Objects.nonNull(baseFsScheme), s"${ConfigKeys.baseFS} not set in configuration")
    require(Objects.nonNull(version), s"${ConfigKeys.version} not set in configuration")
    require(cacheDisabled, s"${ConfigKeys.disableCache} not set to true in configuration")

    import scala.collection.JavaConverters._

    val partitionMappings = conf.iterator.asScala
      .filter(_.getKey.startsWith(VersionedFileSystem.ConfigKeys.partitionMappingPrefix))
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

  object ConfigKeys {
    val baseFS = "fs.versioned.baseFS"
    val version = "fs.versioned.version"
    val partitionMappingPrefix = "fs.versioned.partitionMapping"
    val disableCache = "fs.versioned.impl.disable.cache"
  }

  def partitionMappings(partitions: List[Partition]): Map[String, String] =
    partitions.map { partition =>
      val columnValueHivePath = partition.columnValues.map(cv => cv.column.name + "=" + cv.value).toList.mkString("/")
      s"${ConfigKeys.partitionMappingPrefix}.$columnValueHivePath" -> columnValueHivePath
    }.toMap

  def setPartitionMappings(partitions: List[Partition])(implicit spark: SparkSession): Unit =
    partitionMappings(partitions).foreach {
      case (k, v) =>
        spark.sparkContext.hadoopConfiguration.set(k, v)
    }

  def setUnderlyingScheme(scheme: String)(implicit spark: SparkSession): Unit =
    spark.sparkContext.hadoopConfiguration.set(ConfigKeys.baseFS, scheme)

  def setVersion(version: String)(implicit spark: SparkSession): Unit =
    spark.sparkContext.hadoopConfiguration.set(ConfigKeys.version, version)
}
