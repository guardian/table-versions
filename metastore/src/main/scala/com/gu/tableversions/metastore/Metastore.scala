package com.gu.tableversions.metastore

import com.gu.tableversions.core.{TableName, TableVersion}

trait Metastore[F[_]] {

  def currentVersion(table: TableName): F[TableVersion]

  def syncVersions(table: TableName, latestVersion: TableVersion): F[Unit] = ???

}
