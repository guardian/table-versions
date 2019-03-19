package com.gu.tableversions.dynamodb

import com.gu.tableversions.core._

/**
  * Placeholder for DynamoDB implementation of the table versions service.
  */
class DynamoDbTableStore[F[_]] extends TableVersions[F] {

  override def init(table: TableName): F[Unit] = ???

  override def currentVersions(table: TableName): F[TableVersion] = ???

  override def nextVersions(table: TableName, partitionColumns: List[PartitionColumn]): F[List[PartitionVersion]] = ???

  override def commit(newVersion: TableVersions.TableUpdate): F[TableVersions.CommitResult] = ???

}
