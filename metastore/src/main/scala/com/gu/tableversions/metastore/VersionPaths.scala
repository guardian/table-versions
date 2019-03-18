package com.gu.tableversions.metastore

import java.net.URI

import com.gu.tableversions.core.VersionNumber

/**
  * Encodes the mapping between version numbers and storage paths.
  */
object VersionPaths {

  /**
    * @return a path for a given partition version and base path
    */
  def pathFor(basePath: URI, newVersion: VersionNumber): URI = {
    def normalised(path: String): String = if (path.endsWith("/")) path else path + "/"
    def versioned(path: String): String = if (newVersion.number <= 0) path else s"${path}v${newVersion.number}"
    new URI(versioned(normalised(basePath.toString)))
  }

}
