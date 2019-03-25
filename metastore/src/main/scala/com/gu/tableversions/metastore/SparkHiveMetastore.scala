package com.gu.tableversions.metastore

import cats.effect.IO
import com.gu.tableversions.core.Partition.{ColumnValue, PartitionColumn}
import com.gu.tableversions.core._
import org.apache.spark.sql.SparkSession

/**
  * Concrete implementation of the Metastore API, using Spark and Hive APIs.
  */
// TODO: Parameterise on effect instead of hard coding with IO
class SparkHiveMetastore(implicit spark: SparkSession) extends Metastore[IO] {

  import spark.implicits._
  import SparkHiveMetastore._

  override def currentVersion(table: TableName): IO[Option[TableVersion]] = {
    val tableExists: IO[Boolean] = IO { spark.catalog.tableExists(table.fullyQualifiedName) }

    val partitions: IO[List[String]] =
      IO { spark.sql(s"show partitions ${table.fullyQualifiedName}").collect().toList.map(_.getString(0)) }

    def partitionLocation(partitionExpr: String): IO[String] = {
      val query = s"DESCRIBE FORMATTED ${table.fullyQualifiedName} PARTITION ($partitionExpr)"

      IO(spark.sql(query).where('col_name === "Location").collect().toList.map(_.getString(0)).headOption)
        .map(_.getOrElse(throw new Exception(s"No location information returned for partition $partitionExpr")))
    }

    import cats.implicits._

    for {
      exists <- tableExists
      tablePartitions <- if (exists) partitions else IO(List.empty)
      partitionLocations <- tablePartitions.map { partition =>
        partitionLocation(partition).map { location =>
          partition -> location
        }
      }.sequence

      partitionVersions: List[PartitionVersion] = partitionLocations.map {
        case (partition, location) => PartitionVersion(parsePartition(partition), parseVersion(location))
      }
    } yield if (exists) Some(TableVersion(partitionVersions)) else None
  }

  override def update(table: TableName, changes: Metastore.TableChanges): IO[Unit] = IO.unit // TODO!!!

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
