package com.gu.tableversions.core

import java.time.Instant

//
// Partitions and partition versions
//

/** A partition schema describes the fields used for partitions of a table */
case class PartitionColumn(name: String) extends AnyVal

case class PartitionSchema(columns: List[PartitionColumn])

object PartitionSchema {
  // The special case schema for snapshot tables, i.e. ones without partition, only a single root.
  val snapshot: PartitionSchema = PartitionSchema(Nil)
}

case class ColumnValue(column: PartitionColumn, value: String)

case class Partition(columnValues: Seq[ColumnValue])

case class VersionNumber(number: Int) extends AnyVal

case class PartitionVersion(partition: Partition, version: VersionNumber)

//
// Tables and operations on tables
//

case class TableName(schema: String, name: String) {
  def fullyQualifiedName: String = s"$schema.$name"
}

case class UpdateMessage(content: String)

case class UserId(value: String)

/** A collection of updates to partitions to be applied and tracked as a single atomic change. */
case class TableUpdate(
    userId: UserId,
    message: UpdateMessage,
    timestamp: Instant,
    updatedPartitions: List[PartitionVersion])

/** The complete set of version information for all partitions in a table. */
case class TableVersion(partitionVersions: List[PartitionVersion])
