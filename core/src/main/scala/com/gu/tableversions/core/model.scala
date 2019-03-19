package com.gu.tableversions.core

import java.net.URI

/**
  * A Partition represents a concrete partition of a table, i.e. a partition column with a specific value.
  */
final case class Partition(columnValues: List[Partition.ColumnValue]) {

  /** Given a base path for the table, return the path to the partition. */
  def resolvePath(tableLocation: URI): URI = {
    val partitionsSuffix =
      columnValues.map(columnValue => s"${columnValue.column.name}=${columnValue.value}").mkString("/")
    tableLocation.resolve(partitionsSuffix)
  }
}

object Partition {

  case class PartitionColumn(name: String) extends AnyVal

  case class ColumnValue(column: PartitionColumn, value: String)

  // The special case partition that represents the root partition of a snapshot table.
  val snapshotPartition: Partition = Partition(Nil)

}

/**
  * A partition schema describes the fields used for partitions of a table
  */
final case class PartitionSchema(columns: List[Partition.PartitionColumn])

//
// Versions
//

final case class VersionNumber(number: Int) extends AnyVal

final case class PartitionVersion(partition: Partition, version: VersionNumber)

final case class TableName(schema: String, name: String) {
  def fullyQualifiedName: String = s"$schema.$name"
}

/**
  * The complete set of version information for all partitions in a table.
  */
final case class TableVersion(partitionVersions: List[PartitionVersion])
