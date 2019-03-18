package com.gu.tableversions.core

class InMemoryTableVersions[F[_]] extends TableVersions[F] {

  override def currentVersions(table: TableName): F[TableVersion] = ??? // TODO

  override def nextVersions(table: TableName, partitionColumns: List[PartitionColumn]): F[List[PartitionVersion]] =
    ??? // TODO

  override def commit(newVersion: TableUpdate): F[TableVersions.CommitResult] = ??? // TODO
}
