package com.gu.tableversions.metastore

import com.gu.tableversions.core.{TableName, TableVersion}

class InMemoryMetastore[F[_]] extends Metastore[F] {

  override def currentVersion(table: TableName): F[TableVersion] = ???

}
