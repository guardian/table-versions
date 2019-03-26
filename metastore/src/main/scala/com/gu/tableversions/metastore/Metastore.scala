package com.gu.tableversions.metastore

import java.net.URI

import com.gu.tableversions.core._
import com.gu.tableversions.metastore.Metastore.TableChanges

trait Metastore[F[_]] {

  /**
    * Describe the current table in the Metastore interpreted in terms of version information.
    *
    * @param table The table to query
    * @return the table version information, or a failure if the table doesn't exist or can't be queried.
    */
  def currentVersion(table: TableName): F[TableVersion]

  /**
    * Apply the given changes to the table in the Hive Metastore.
    *
    * @param table The table to update
    * @param changes The changes that need to be applied to the table
    * @return failure if the table is unknown or can't be updated.
    */
  def update(table: TableName, changes: TableChanges): F[Unit]

  /**
    * @return the set of changes that need to be applied to the Metastore to convert the `current` table
    *         to the `target` table.
    */
  def computeChanges(current: TableVersion, target: TableVersion): TableChanges =
    Metastore.computeChanges(current, target)

}

object Metastore {

  final case class TableChanges(operations: List[TableOperation])

  sealed trait TableOperation

  object TableOperation {
    final case class AddPartition(partitionVersion: PartitionVersion) extends TableOperation
    final case class UpdatePartitionVersion(partitionVersion: PartitionVersion) extends TableOperation
    final case class RemovePartition(partition: Partition) extends TableOperation
    final case class UpdateTableLocation(tableLocation: URI, versionNumber: VersionNumber) extends TableOperation
  }

  def computeChanges(current: TableVersion, target: TableVersion): TableChanges = ???

}
