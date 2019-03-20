package com.gu.tableversions.examples

import java.sql.{Date, Timestamp}
import java.time.Instant

import cats.effect.IO
import com.gu.tableversions.core.TableVersions.{TableUpdate, UpdateMessage, UserId}
import com.gu.tableversions.core._
import com.gu.tableversions.examples.DatePartitionedTableLoader.Pageview
import com.gu.tableversions.metastore.{Metastore, VersionPaths}
import com.gu.tableversions.spark.VersionedDataset
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * This example contains code that writes example event data to a table that has a single date partition.
  * It demonstrates how individual partitions in such a table can be updated using versioning.
  */
class DatePartitionedTableLoader(table: TableDefinition, tableVersions: TableVersions[IO], metastore: Metastore[IO])(
    implicit val spark: SparkSession)
    extends LazyLogging {

  import spark.implicits._

  def initTable(): Unit = {
    // Create table schema in metastore
    val ddl = s"""CREATE EXTERNAL TABLE IF NOT EXISTS ${table.name.fullyQualifiedName} (
                 |  `id` string,
                 |  `path` string,
                 |  `timestamp` timestamp
                 |)
                 |PARTITIONED BY (`date` date)
                 |STORED AS parquet
                 |LOCATION '${table.location}'
    """.stripMargin

    spark.sql(ddl)

    // Initialise version tracking for table
    tableVersions.init(table.name).unsafeRunSync()
  }

  def pageviews(): Dataset[Pageview] =
    spark.table(table.name.fullyQualifiedName).as[Pageview]

  def insert(dataset: Dataset[Pageview], message: String): Unit = {

    // Find the partition values in the given dataset
    val datasetPartitions: List[Partition] = VersionedDataset.partitionValues(dataset, table.partitionSchema)

    val update: IO[(TableVersion, Metastore.TableChanges)] = for {
      // Get next version numbers for the partitions of the dataset
      workingVersions <- tableVersions.nextVersions(table.name, datasetPartitions)

      // Resolve the path that each partition should be written to, based on their version
      partitionPaths = VersionPaths.resolveVersionedPartitionPaths(workingVersions, table.location)

      // Write dataset partitions to these paths.
      _ <- VersionedDataset.writeVersionedPartitions(dataset, partitionPaths)

      // Commit partitions
      _ <- tableVersions.commit(
        TableUpdate(UserId("test user"), UpdateMessage(message), Instant.now(), workingVersions))

      // Get latest version details and Metastore table details and sync the Metastore to match,
      // effectively switching the table to the new version.
      latestTableVersion <- tableVersions.currentVersion(table.name)
      metastoreTable <- metastore.currentVersion(table.name)
      metastoreChanges = Metastore.computeChanges(metastoreTable, latestTableVersion)

      // Sync Metastore to match
      _ <- metastore.update(table.name, metastoreChanges)

    } yield (latestTableVersion, metastoreChanges)

    val (latestTableVersion, metastoreChanges) = update.unsafeRunSync()

    logger.info(s"Updated table $table, new version details:\n$latestTableVersion")
    logger.info(s"Applied the the following changes to sync the Metastore:\n$metastoreChanges")
  }

}

object DatePartitionedTableLoader {

  case class Pageview(id: String, path: String, timestamp: Timestamp, date: Date)

  object Pageview {

    def apply(id: String, path: String, timestamp: Timestamp): Pageview =
      Pageview(id, path, timestamp, DateTime.timestampToUtcDate(timestamp))

  }

}
