package com.gu.tableversions.metastore

import com.gu.tableversions.core.{TableName, TableVersion}

trait Metastore[F[_]] {

  /**
    * Describe the current table in the Metastore interpreted in terms of version information.
    *
    * @param table The table to query
    */
  def currentVersion(table: TableName): F[TableVersion]

  /**
    * Update table in metastore according to the given version information.
    *
    * @param table The table to update
    * @param latestVersion The version information to apply
    */
  def syncVersions(table: TableName, latestVersion: TableVersion): F[Unit]

}
