package com.gu.tableversions.spark

import java.io.File
import java.net.URI

import cats.effect.Sync
import cats.implicits._
import com.gu.tableversions.core.Partition.{ColumnValue, PartitionColumn}
import com.gu.tableversions.core._
import com.gu.tableversions.metastore.Metastore.TableOperation
import com.gu.tableversions.metastore.Metastore.TableOperation._
import com.gu.tableversions.metastore.{Metastore, VersionPaths}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

/**
  * Concrete implementation of the Metastore API, using Spark and Hive APIs.
  */
class SparkHiveMetastore[F[_]](implicit spark: SparkSession, F: Sync[F]) extends Metastore[F] with LazyLogging {

  import SparkHiveMetastore._
  import spark.implicits._

  override def currentVersion(table: TableName): F[TableVersion] = {

    val partitionedTableVersion: F[TableVersion] = for {
      partitions <- listPartitions(table)
      partitionLocations <- partitions.traverse { partition =>
        partitionLocation(table, toPartitionExpr(partition)).map(location => partition -> location)
      }

      partitionVersions = partitionLocations.map {
        case (partition, location) => parsePartition(partition) -> parseVersion(location)
      }
    } yield TableVersion(partitionVersions.toMap)

    val snapshotTableVersion: F[TableVersion] = for {
      tableLocation <- findTableLocation(table)
      versionNumber = parseVersion(tableLocation)
    } yield TableVersion(Map(Partition.snapshotPartition -> versionNumber))

    // Choose the calculation to perform based on whether we have a partitioned table or not
    isPartitioned(table).flatMap(partitioned => if (partitioned) partitionedTableVersion else snapshotTableVersion)
  }

  override def update(table: TableName, changes: Metastore.TableChanges): F[Unit] =
    changes.operations.traverse_(appliedOp(table))

  private def appliedOp(table: TableName)(operation: TableOperation): F[Unit] =
    operation match {
      case AddPartition(partition, version)           => addPartition(table, partition, version)
      case UpdatePartitionVersion(partition, version) => updatePartitionVersion(table, partition, version)
      case RemovePartition(partition)                 => removePartition(table, partition)
      case UpdateTableVersion(versionNumber)          => updateTableLocation(table, versionNumber)
    }

  private def addPartition(table: TableName, partition: Partition, version: Version): F[Unit] = {

    def addPartition(partitionExpr: String, partitionLocation: URI): F[Unit] = {
      val addPartitionQuery =
        s"ALTER TABLE ${table.fullyQualifiedName} ADD IF NOT EXISTS PARTITION $partitionExpr LOCATION '$partitionLocation'"
      performUpdate(s"Adding partition to table ${table.fullyQualifiedName}", addPartitionQuery)
    }

    val partitionExpr = toHivePartitionExpr(partition)

    versionedPartitionLocation(table, partition, version).flatMap(location =>
      addPartition(partitionExpr, location).void)
  }

  private def updatePartitionVersion(table: TableName, partition: Partition, version: Version): F[Unit] = {

    def updatePartition(partitionExpr: String, partitionLocation: URI): F[Unit] = {
      val updatePartitionQuery =
        s"ALTER TABLE ${table.fullyQualifiedName} PARTITION $partitionExpr SET LOCATION '$partitionLocation'"
      performUpdate(s"Updating partition version of partition ${partition}", updatePartitionQuery)
    }

    val partitionExpr = toHivePartitionExpr(partition)

    versionedPartitionLocation(table, partition, version).flatMap(location =>
      updatePartition(partitionExpr, location).void)
  }

  private def removePartition(table: TableName, partition: Partition): F[Unit] = {
    val partitionExpr = toHivePartitionExpr(partition)
    val removePartitionQuery =
      s"ALTER TABLE ${table.fullyQualifiedName} DROP IF EXISTS PARTITION $partitionExpr"
    performUpdate(s"Removing partition $partition from table ${table.fullyQualifiedName}", removePartitionQuery)
  }

  private def updateTableLocation(table: TableName, version: Version): F[Unit] = {
    def updateLocation(tableLocation: URI): F[Unit] = {
      val versionedPath = VersionPaths.pathFor(tableLocation, version)
      val updateQuery = s"ALTER TABLE ${table.fullyQualifiedName} SET LOCATION '$versionedPath'"
      performUpdate(s"Updating table location of table ${table.fullyQualifiedName}", updateQuery)
    }

    findTableLocation(table).map(versionedToBasePath).flatMap(updateLocation).void
  }

  private def versionedPartitionLocation(table: TableName, partition: Partition, version: Version): F[URI] =
    for {
      tableLocation <- findTableLocation(table)
      partitionLocation <- F.delay(partition.resolvePath(tableLocation))
      versionedPartitionLocation <- F.delay(VersionPaths.pathFor(partitionLocation, version))
    } yield versionedPartitionLocation

  private def performUpdate(description: String, query: String): F[Unit] =
    F.delay {
      logger.info(s"$description using query: $query")
      spark.sql(query)
    }.void

  private def findTableLocation(table: TableName): F[URI] = {
    F.delay {
      spark
        .sql(s"DESCRIBE FORMATTED ${table.fullyQualifiedName}")
        .where('col_name === "Location")
        .collect()
        .map(_.getString(1))
        .headOption
        .map(new URI(_))
        .getOrElse(throw new Exception(s"No location information returned for table ${table.fullyQualifiedName}"))
    }

  }

  private def listPartitions(table: TableName): F[List[String]] =
    F.delay { spark.sql(s"show partitions ${table.fullyQualifiedName}").collect().toList.map(_.getString(0)) }

  private def partitionLocation(table: TableName, partitionExpr: String): F[URI] =
    F.delay {
      spark
        .sql(s"DESCRIBE FORMATTED ${table.fullyQualifiedName} PARTITION $partitionExpr")
        .where('col_name === "Location")
        .collect()
        .map(_.getAs[String]("data_type"))
        .headOption
        .map(new URI(_))
        .getOrElse(throw new Exception(
          s"No location information returned for partition $partitionExpr on table ${table.fullyQualifiedName}"))
    }

  private def isPartitioned(table: TableName): F[Boolean] =
    // We have to interpret the strange format returned by DESCRIBE queries, which, if the table is partitioned,
    // contains rows like:
    //
    // |# Partition Information|         |               |
    // |# col_name             |data_type|comment        |
    // |date                   |date     |null           |
    F.delay {
      spark
        .sql(s"DESCRIBE ${table.fullyQualifiedName}")
        .collect()
        .flatMap(row => Option(row.getAs[String]("col_name")))
        .contains("# Partition Information")
    }

}

object SparkHiveMetastore {

  private[spark] def toPartitionExpr(partitionPath: String): String =
    toHivePartitionExpr(parsePartition(partitionPath))

  private[spark] def toHivePartitionExpr(partition: Partition): String =
    partition.columnValues
      .map(columnValue => s"${columnValue.column.name}='${columnValue.value}'")
      .mkString("(", ",", ")")

  private val ColumnValueRegex = """(?x)
                                   |([a-z_]+)  # column name
                                   |=
                                   |(.+)       # column value
                                 """.stripMargin.r

  private[spark] def parsePartition(partitionStr: String): Partition = {
    def parseColumnValue(str: String): ColumnValue = str match {
      case ColumnValueRegex(columnName, value) => ColumnValue(PartitionColumn(columnName), value)
      case _                                   => throw new Exception(s"Invalid partition string: $partitionStr")
    }
    val parts = partitionStr.split("/").toList
    val columnValues = parts.map(parseColumnValue)
    Partition(columnValues)
  }

  private val VersionRegex = "v(\\d+)$".r

  private[spark] def parseVersion(location: URI): Version = {
    val maybeVersionStr = location.toString.split("/").lastOption
    maybeVersionStr match {
      case Some(VersionRegex(versionStr)) => Version(versionStr.toInt)
      case _                              => Version(0)
    }
  }

  private[spark] def versionedToBasePath(location: URI): URI = {
    def parentPath(uri: URI): URI = {
      val parentPath = new File(uri.getPath).getParent
      new URI(uri.getScheme, uri.getUserInfo, uri.getHost, uri.getPort, parentPath, uri.getQuery, uri.getFragment)
    }
    if (location.toString.matches(".*/v(\\d+)$"))
      parentPath(location)
    else location
  }

}
