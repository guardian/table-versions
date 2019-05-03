package com.gu.tableversions.spark

import java.net.URI
import java.util.Objects

import com.gu.tableversions.core.Version
import com.gu.tableversions.spark.VersionedFileSystem.ConfigKeys
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration

class VersionedFileSystem extends ProxyFileSystem with LazyLogging {

  override def initialize(name: URI, conf: Configuration): Unit = {
    val cacheDisabled = conf.getBoolean(ConfigKeys.disableCache, false)
    val baseFsScheme = conf.get(ConfigKeys.baseFS)
    val versionString = conf.get(ConfigKeys.version)
    val version = Version.parse(versionString)

    logger.info(s"Cache disabled = $cacheDisabled")
    logger.info(s"Base filesystem scheme = $baseFsScheme")
    logger.info(s"Version string = $versionString")

    require(Objects.nonNull(baseFsScheme), s"${ConfigKeys.baseFS} not set in configuration")
    require(Objects.nonNull(versionString), s"${ConfigKeys.version} not set in configuration")
    require(version.isRight, s"Unable to parse version label $versionString. ${version.left.get.getMessage}")
    require(cacheDisabled, s"${ConfigKeys.disableCache} not set to true in configuration")

    // When initialising the versioned filesystem, we need to extract the
    // URI for the base filesystem, which is accessible under the scheme
    // specific part of the URI.
    val baseURI = new URI(baseFsScheme, null, name.getSchemeSpecificPart, null, null)
    val pathMapper = new VersionedPathMapper(baseFsScheme, version.right.get)

    initialiseProxyFileSystem(baseURI, pathMapper, conf)
  }
}

object VersionedFileSystem {

  val scheme = "versioned"

  object ConfigKeys {
    val baseFS = "fs.versioned.baseFS"
    val version = "fs.versioned.version"
    val disableCache = "fs.versioned.impl.disable.cache"
  }

  def sparkConfig(baseFsScheme: String, version: Version): Map[String, String] =
    Map(
      ConfigKeys.baseFS -> baseFsScheme,
      ConfigKeys.version -> version.label
    )
}
