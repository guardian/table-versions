package com.gu.tableversions.metastore

import java.net.URI

import com.gu.tableversions.core.{PartitionVersion, TableName, TableVersion, VersionNumber}
import com.gu.tableversions.metastore.Metastore.TableChanges

trait Metastore[F[_]] {

  /**
    * Describe the current table in the Metastore interpreted in terms of version information.
    *
    * @param table The table to query
    */
  def currentVersion(table: TableName): F[TableVersion]

  /**
    * Apply the given changes to the table in the Hive Metastore.
    *
    * @param table The table to update
    * @param changes The changes that need to be applied to the table
    */
  def update(table: TableName, changes: TableChanges): F[Unit]

}

object Metastore {

  sealed trait TableChanges
  private[metastore] final case class TableChangeOperations(operations: TableOperation) extends TableChanges

  private[metastore] sealed trait TableOperation
  final case class AddPartitionOperation(partition: PartitionVersion) extends TableOperation
  final case class RemovePartitionOperation(partition: PartitionVersion)
  final case class UpdateTableLocation(tableLocation: URI, versionNumber: VersionNumber)

  /**
    * @return the set of changes that need to be applied to the Metastore to convert the `current` table
    *         to the `target` table.
    */
  def computeChanges(current: TableVersion, target: TableVersion): TableChanges = ???

}
