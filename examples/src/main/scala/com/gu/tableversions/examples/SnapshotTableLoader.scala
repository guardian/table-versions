package com.gu.tableversions.examples

import java.time.Instant

import cats.effect.IO
import com.gu.tableversions.core.TableVersions._
import com.gu.tableversions.core._
import com.gu.tableversions.examples.SnapshotTableLoader.User
import com.gu.tableversions.metastore.Metastore.TableChanges
import com.gu.tableversions.metastore.{Metastore, VersionPaths}
import com.gu.tableversions.spark.VersionedDataset
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * This is an example of loading data into a 'snapshot' table, that is, a table where we replace all the content
  * every time we write to it (no partial updates).
  */
class SnapshotTableLoader(table: TableDefinition, tableVersions: TableVersions[IO], metastore: Metastore[IO])(
    implicit val spark: SparkSession)
    extends LazyLogging {

  import spark.implicits._

  def initTable(): Unit = {
    // Create table schema in metastore
    val ddl = s"""CREATE EXTERNAL TABLE IF NOT EXISTS ${table.name.fullyQualifiedName} (
                 |  `id` string,
                 |  `name` string,
                 |  `email` string
                 |)
                 |STORED AS parquet
                 |LOCATION '${table.location}'
    """.stripMargin

    spark.sql(ddl)

    // Initialise version tracking for table
    tableVersions.init(table.name).unsafeRunSync()
  }

  def users(): Dataset[User] =
    spark.table(table.name.fullyQualifiedName).as[User]

  def insert(dataset: Dataset[User], message: String): Unit = {

    // Find the partition values in the given dataset
    val datasetPartitions: List[Partition] = VersionedDataset.partitionValues(dataset, table.partitionSchema)

    val update: IO[(TableVersion, TableChanges)] = for {
      // Get next version numbers for the partitions of the dataset
      workingVersions <- tableVersions.nextVersions(table.name, datasetPartitions)

      // Resolve the path that each partition should be written to, based on their version
      partitionPaths = VersionPaths.resolveVersionedPartitionPaths(workingVersions, table.location)

      // Write Spark dataset to the versioned path
      _ <- VersionedDataset.writeVersionedPartitions(dataset, partitionPaths)

      // Commit written version
      _ <- tableVersions.commit(
        TableUpdate(UserId("test user"), UpdateMessage(message), Instant.now(), workingVersions))

      // Get latest version details and Metastore table details and sync the Metastore to match,
      // effectively switching the table to the new version.
      latestTableVersion <- tableVersions.currentVersion(table.name)
      metastoreVersion <- metastore.currentVersion(table.name)
      metastoreUpdate = Metastore.computeChanges(latestTableVersion, metastoreVersion)

      // Sync Metastore to match
      _ <- metastore.update(table.name, metastoreUpdate)

    } yield (latestTableVersion, metastoreUpdate)

    val (latestVersion, metastoreChanges) = update.unsafeRunSync()

    logger.info(s"Updated table $table, new version details:\n$latestVersion")
    logger.info(s"Applied the the following changes to sync the Metastore:\n$metastoreChanges")
  }

}

object SnapshotTableLoader {

  case class User(id: String, name: String, email: String)

}
