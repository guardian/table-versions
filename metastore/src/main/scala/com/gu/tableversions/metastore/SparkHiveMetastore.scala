package com.gu.tableversions.metastore

import java.net.URI

import cats.effect.IO
import cats.implicits._
import com.gu.tableversions.core.Partition.{ColumnValue, PartitionColumn}
import com.gu.tableversions.core._
import com.gu.tableversions.metastore.Metastore.TableOperation
import com.gu.tableversions.metastore.Metastore.TableOperation._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

/**
  * Concrete implementation of the Metastore API, using Spark and Hive APIs.
  */
// TODO: Parameterise on effect instead of hard coding with IO
class SparkHiveMetastore(implicit spark: SparkSession) extends Metastore[IO] with LazyLogging {

  import SparkHiveMetastore._
  import spark.implicits._

  override def currentVersion(table: TableName): IO[TableVersion] = {

    val partitionedTableVersion: IO[TableVersion] = for {
      partitions <- listPartitions(table)
      partitionLocations <- partitions.map { partition =>
        partitionLocation(table, toPartitionExpr(partition)).map(location => partition -> location)
      }.sequence

      partitionVersions: List[PartitionVersion] = partitionLocations.map {
        case (partition, location) => PartitionVersion(parsePartition(partition), parseVersion(location))
      }
    } yield TableVersion(partitionVersions)

    val snapshotTableVersion: IO[TableVersion] = for {
      tableLocation <- findTableLocation(table)
      versionNumber = parseVersion(tableLocation)
    } yield TableVersion(List(PartitionVersion(Partition.snapshotPartition, versionNumber)))

    // Choose the calculation to perform based on whether we have a partitioned table or not
    isPartitioned(table).flatMap(partitioned => if (partitioned) partitionedTableVersion else snapshotTableVersion)
  }

  override def update(table: TableName, changes: Metastore.TableChanges): IO[Unit] =
    changes.operations.map(appliedOp(table)).sequence.void

  private def appliedOp(table: TableName)(operation: TableOperation): IO[Unit] =
    operation match {
      case AddPartition(partitionVersion)                    => addPartition(table, partitionVersion)
      case UpdatePartitionVersion(partitionVersion)          => updatePartitionVersion(table, partitionVersion)
      case RemovePartition(partition)                        => removePartition(table, partition)
      case UpdateTableLocation(tableLocation, versionNumber) => updateTableLocation(table, tableLocation, versionNumber)
    }

  private def addPartition(table: TableName, partitionVersion: PartitionVersion): IO[Unit] = {

    def addPartition(partitionExpr: String, partitionLocation: URI): IO[Unit] = {
      val addPartitionQuery =
        s"ALTER TABLE ${table.fullyQualifiedName} ADD IF NOT EXISTS PARTITION $partitionExpr LOCATION '$partitionLocation'"
      performUpdate(s"Adding partition to table ${table.fullyQualifiedName}", addPartitionQuery)
    }

    val partitionExpr = toHivePartitionExpr(partitionVersion.partition)

    versionedPartitionLocation(table, partitionVersion).flatMap(location => addPartition(partitionExpr, location).void)
  }

  private def updatePartitionVersion(table: TableName, partitionVersion: PartitionVersion): IO[Unit] = {

    def updatePartition(partitionExpr: String, partitionLocation: URI): IO[Unit] = {
      val updatePartitionQuery =
        s"ALTER TABLE ${table.fullyQualifiedName} PARTITION $partitionExpr SET LOCATION '$partitionLocation'"
      performUpdate(s"Updating partition version of partition ${partitionVersion.partition}", updatePartitionQuery)
    }

    val partitionExpr = toHivePartitionExpr(partitionVersion.partition)

    versionedPartitionLocation(table, partitionVersion).flatMap(location =>
      updatePartition(partitionExpr, location).void)
  }

  private def removePartition(table: TableName, partition: Partition): IO[Unit] = {
    val partitionExpr = toHivePartitionExpr(partition)
    val removePartitionQuery =
      s"ALTER TABLE ${table.fullyQualifiedName} DROP IF EXISTS PARTITION $partitionExpr"
    performUpdate(s"Removing partition $partition from table ${table.fullyQualifiedName}", removePartitionQuery)
  }

  // TODO: Separate out the non-IO bits of this
  private def versionedPartitionLocation(table: TableName, partitionVersion: PartitionVersion): IO[URI] =
    for {
      tableLocation <- findTableLocation(table).map(new URI(_))
      partitionLocation = partitionVersion.partition.resolvePath(tableLocation)
      versionedPartitionLocation = VersionPaths.pathFor(partitionLocation, partitionVersion.version)
    } yield versionedPartitionLocation

  private def updateTableLocation(table: TableName, tableLocation: URI, version: VersionNumber): IO[Unit] = {
    val versionedPath = VersionPaths.pathFor(tableLocation, version)
    val updateQuery = s"ALTER TABLE ${table.fullyQualifiedName} SET LOCATION '$versionedPath'"
    performUpdate(s"Updating table location of table ${table.fullyQualifiedName}", updateQuery)
  }

  private def performUpdate(description: String, query: String): IO[Unit] =
    IO {
      logger.info(s"$description using query: $query")
      spark.sql(query)
    }.void

  private def findTableLocation(table: TableName): IO[String] = {
    val query = s"DESCRIBE FORMATTED ${table.fullyQualifiedName}"

    IO(spark.sql(query).where('col_name === "Location").collect().toList.map(_.getString(1)).headOption)
      .map(_.getOrElse(throw new Exception(s"No location information returned for table ${table.fullyQualifiedName}")))
  }

  private def listPartitions(table: TableName): IO[List[String]] =
    IO { spark.sql(s"show partitions ${table.fullyQualifiedName}").collect().toList.map(_.getString(0)) }

  private def partitionLocation(table: TableName, partitionExpr: String): IO[String] =
    IO {
      spark
        .sql(s"DESCRIBE FORMATTED ${table.fullyQualifiedName} PARTITION $partitionExpr")
        .where('col_name === "Location")
        .collect()
        .map(_.getAs[String]("data_type"))
        .headOption
    }.map(_.getOrElse(throw new Exception(
      s"No location information returned for partition $partitionExpr on table ${table.fullyQualifiedName}")))

  private def isPartitioned(table: TableName): IO[Boolean] = {
    // We have to interpret the strange format returned by DESCRIBE queries, which, if the table is partitioned,
    // contains rows like:
    //
    // |# Partition Information|         |               |
    // |# col_name             |data_type|comment        |
    // |date                   |date     |null           |

    IO {
      spark
        .sql(s"DESCRIBE ${table.fullyQualifiedName}")
        .collect()
        .flatMap(row => Option(row.getAs[String]("col_name")))
        .contains("# Partition Information")
    }
  }

}

object SparkHiveMetastore {

  private[metastore] def toPartitionExpr(partitionPath: String): String =
    toHivePartitionExpr(parsePartition(partitionPath))

  private[metastore] def toHivePartitionExpr(partition: Partition): String =
    partition.columnValues
      .map(columnValue => s"${columnValue.column.name}='${columnValue.value}'")
      .mkString("(", ",", ")")

  private val ColumnValueRegex = "([a-z_]+)=(.+)".r

  private[metastore] def parsePartition(partitionStr: String): Partition = {
    def parseColumnValue(str: String): ColumnValue = str match {
      case ColumnValueRegex(columnName, value) => ColumnValue(PartitionColumn(columnName), value)
      case _                                   => throw new Exception(s"Invalid partition string: $partitionStr")
    }
    val parts = partitionStr.split("/").toList
    val columnValues = parts.map(parseColumnValue)
    Partition(columnValues)
  }

  private val VersionRegex = "v(\\d+)".r

  private[metastore] def parseVersion(location: String): VersionNumber = {
    val maybeVersionStr = location.split("/").lastOption
    maybeVersionStr match {
      case Some(VersionRegex(versionStr)) => VersionNumber(versionStr.toInt)
      case _                              => VersionNumber(0)
    }
  }

}
