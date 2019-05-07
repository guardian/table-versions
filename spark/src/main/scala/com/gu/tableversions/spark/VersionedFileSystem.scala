package com.gu.tableversions.spark

import java.net.URI
import java.util.Objects

import com.gu.tableversions.core.{Partition, Version}
import com.gu.tableversions.spark.VersionedFileSystem.ConfigKeys
import io.circe.parser._
import io.circe.{Encoder, KeyEncoder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

class VersionedFileSystem extends ProxyFileSystem {

  override def initialize(name: URI, conf: Configuration): Unit = {
    val cacheDisabled = conf.getBoolean(ConfigKeys.disableCache, false)
    val baseFsScheme = conf.get(ConfigKeys.baseFS)

    conf.

    require(Objects.nonNull(baseFsScheme), s"${ConfigKeys.baseFS} not set in configuration")
    require(cacheDisabled, s"${ConfigKeys.disableCache} not set to true in configuration")

    val config = readConfig().right.get

    // When initialising the versioned filesystem, we need to extract the
    // URI for the base filesystem, which is accessible under the scheme
    // specific part of the URI.
    val baseURI = new URI(baseFsScheme, null, name.getSchemeSpecificPart, null, null)
    val pathMapper = new VersionedPathMapper(baseFsScheme, partitionVersions)

    initialiseProxyFileSystem(baseURI, pathMapper, conf)
  }

  private def readConfig(): Either[Error, Map[String, String]] = {
    val rawPartitionVersions = ???
    decode[Map[String, String]](rawPartitionVersions)
  }
}

object VersionedFileSystem {

  import io.circe.generic.auto._
  import io.circe.syntax._

  case class VersionedFileSystemConfig(partitionVersions: Map[Partition, Version])

  val scheme = "versioned"

  implicit def partitionEncoder: KeyEncoder[Partition] =
    KeyEncoder.instance { partition =>
      partition.columnValues.map(cv => s"${cv.column.name}=${cv.value}").toList.mkString("/")
    }

  implicit def versionEncoder: Encoder[Version] =
    Encoder.encodeString.contramap(_.label)

  object ConfigKeys {
    val baseFS = "fs.versioned.baseFS"
    val disableCache = "fs.versioned.impl.disable.cache"
  }

  def writeConfig(config: VersionedFileSystemConfig, path: URI, hadoopConfiguration: Configuration): Unit = {
    val fs = FileSystem.get(path, hadoopConfiguration)
    val os = fs.create(new Path(path))

    try {
      val jsonBytes = config.asJson.noSpaces.getBytes("UTF-8")
      os.writeInt(jsonBytes.length)
      os.write(jsonBytes)
    } finally {
      os.close()
    }
  }

  def setUnderlyingScheme(scheme: String)(implicit spark: SparkSession): Unit =
    spark.sparkContext.hadoopConfiguration.set(ConfigKeys.baseFS, scheme)
}
