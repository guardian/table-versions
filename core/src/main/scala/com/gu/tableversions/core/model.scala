package com.gu.tableversions.core

import java.net.URI

import cats.data.NonEmptyList

/**
  * A Partition represents a concrete partition of a table, i.e. a partition column with a specific value.
  */
final case class Partition(columnValues: NonEmptyList[Partition.ColumnValue]) {

  /** Given a base path for the table, return the path to the partition. */
  def resolvePath(tableLocation: URI): URI = {
    def normalised(dir: URI): URI = if (dir.toString.endsWith("/")) dir else new URI(dir.toString + "/")

    val partitionsSuffix =
      columnValues.map(columnValue => s"${columnValue.column.name}=${columnValue.value}").toList.mkString("", "/", "/")
    normalised(tableLocation).resolve(partitionsSuffix)
  }
}

object Partition {

  /** Convenience constructor for single column partitions. */
  def apply(columnValue: ColumnValue): Partition = Partition(NonEmptyList.one(columnValue))

  /** Convenience constructor for single column partitions. */
  def apply(column: PartitionColumn, value: String): Partition = Partition(NonEmptyList.one(ColumnValue(column, value)))

  /** Convenience constructor for multiple partitions. */
  def apply(first: ColumnValue, rest: ColumnValue*): Partition = Partition(NonEmptyList(first, rest.toList))

  case class PartitionColumn(name: String)

  case class ColumnValue(column: PartitionColumn, value: String)

}

/**
  * A partition schema describes the fields used for partitions of a table
  */
final case class PartitionSchema(columns: List[Partition.PartitionColumn])

object PartitionSchema {

  // The special case partition that represents the root partition of a snapshot table.
  val snapshot: PartitionSchema = PartitionSchema(Nil)

}

//
// Tables
//

final case class TableName(schema: String, name: String) {
  def fullyQualifiedName: String = s"$schema.$name"
}

final case class TableDefinition(name: TableName, location: URI, partitionSchema: PartitionSchema) {
  def isSnapshot: Boolean = partitionSchema == PartitionSchema.snapshot
}

/**
  * The complete set of version information for all partitions in a table.
  */
sealed trait TableVersion
final case class PartitionedTableVersion(partitionVersions: Map[Partition, Version]) extends TableVersion
final case class SnapshotTableVersion(version: Version) extends TableVersion
