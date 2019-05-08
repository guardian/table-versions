package com.gu.tableversions.spark.filesystem

import java.net.URI
import java.time.Instant
import java.util.Objects

import com.gu.tableversions.core.{Partition, Version}
import com.gu.tableversions.spark.filesystem.VersionedFileSystem.ConfigKeys
import com.typesafe.scalalogging.LazyLogging
import io.circe.parser._
import io.circe.{Decoder, Encoder, KeyDecoder, KeyEncoder}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

class VersionedFileSystem extends ProxyFileSystem with LazyLogging {

  override def initialize(path: URI, conf: Configuration): Unit = {
    val cacheDisabled = conf.getBoolean(ConfigKeys.disableCache, false)
    val baseFsScheme = conf.get(ConfigKeys.baseFS)
    val configDirectory = conf.get(ConfigKeys.configDirectory)

    require(Objects.nonNull(baseFsScheme), s"${ConfigKeys.baseFS} not set in configuration")
    require(Objects.nonNull(configDirectory), s"${ConfigKeys.configDirectory} not set in configuration")
    require(cacheDisabled, s"${ConfigKeys.disableCache} not set to true in configuration")

    logger.info(s"Cache disabled = $cacheDisabled")
    logger.info(s"Base filesystem scheme = $baseFsScheme")
    logger.info(s"Config directory = $configDirectory")

    // When initialising the versioned filesystem we need to swap the versioned:// prefix
    // in the URI passed during initialisation to the base scheme.
    val baseURI = new URI(baseFsScheme, null, path.getSchemeSpecificPart, null, null)

    VersionedFileSystem.readConfig(new URI(configDirectory), conf) match {
      case Right(config) =>
        val pathMapper = new VersionedPathMapper(baseFsScheme, config.partitionVersions.map {
          case (p, v) => p.toString -> v.label
        })

        initialiseProxyFileSystem(baseURI, pathMapper, conf)

      case Left(e) => throw e
    }
  }
}

object VersionedFileSystem extends LazyLogging {

  val scheme = "versioned"
  val configFilename = "_vfsconfig"
  val configEncoding = "UTF-8"

  object ConfigKeys {
    val baseFS = "fs.versioned.baseFS"
    val disableCache = "fs.versioned.impl.disable.cache"
    val configDirectory = "fs.versioned.configDirectory"
  }

  import cats.syntax.either._
  import io.circe.Decoder._
  import io.circe.generic.auto._
  import io.circe.syntax._

  case class VersionedFileSystemConfig(partitionVersions: Map[Partition, Version])

  implicit def partitionKeyDecoder: KeyDecoder[Partition] =
    KeyDecoder.instance(s => Partition.parse(s).toOption)

  implicit def versionDecoder: Decoder[Version] =
    Decoder.decodeString.emap(s => Version.parse(s).leftMap(_.getMessage))

  implicit def instantDecoder: Decoder[Instant] = Decoder.decodeString.emap { str =>
    Either.catchNonFatal(Instant.parse(str)).leftMap(t => s"Unable to parse instant '$str': " + t.getMessage)
  }

  implicit def partitionEncoder: KeyEncoder[Partition] =
    KeyEncoder.instance { partition =>
      partition.columnValues.map(cv => s"${cv.column.name}=${cv.value}").toList.mkString("/")
    }

  implicit def versionEncoder: Encoder[Version] =
    Encoder.encodeString.contramap(_.label)

  def writeConfig(config: VersionedFileSystemConfig, hadoopConfiguration: Configuration): Unit = {
    val configDirectoryPath = hadoopConfiguration.get(ConfigKeys.configDirectory)
    val configFile = configFilename(new URI(configDirectoryPath))
    logger.info(s"Writing config with ${config.partitionVersions.size} partition versions to file: $configFile")

    val fs = FileSystem.get(configFile, hadoopConfiguration)
    val os = fs.create(new Path(configFile))
    try {
      val configJson = config.asJson
      val jsonBytes = configJson.noSpaces.getBytes(configEncoding)
      os.write(jsonBytes)
      os.flush()
    } finally {
      os.close()
    }
  }

  def readConfig(configDir: URI, hadoopConfiguration: Configuration): Either[Throwable, VersionedFileSystemConfig] = {
    val path = configFilename(configDir)
    logger.info(s"Reading config from path: $path")

    val fs = FileSystem.get(path, hadoopConfiguration)
    for {
      is <- Either.catchNonFatal(fs.open(new Path(path.resolve(VersionedFileSystem.configFilename))))
      configString <- Either.catchNonFatal(IOUtils.toString(is, VersionedFileSystem.configEncoding))
      config <- decode[VersionedFileSystemConfig](configString)
    } yield config
  }

  def configFilename(configDir: URI): URI = {
    val normalizedConfigDir: URI =
      if (configDir.toString.endsWith("/")) configDir else new URI(configDir.toString + "/")
    normalizedConfigDir.resolve(configFilename)
  }

  def setConfigDirectory(path: URI)(implicit spark: SparkSession): Unit = {
    logger.info(s"Setting config directory to path: $path")
    spark.sparkContext.hadoopConfiguration.set(ConfigKeys.configDirectory, path.toString)
  }

}
