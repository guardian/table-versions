package com.gu.tableversions.examples

import java.net.URI
import java.time.Instant

import cats.effect.IO
import com.gu.tableversions.core.TableVersions._
import com.gu.tableversions.core._
import com.gu.tableversions.examples.SnapshotTableLoader.User
import com.gu.tableversions.metastore.{Metastore, VersionPaths}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
  * This is an example of loading data into a 'snapshot' table, that is, a table where we replace all the content
  * every time we write to it (no partial updates).
  *
  * @param table The fully qualified table name
  * @param tableLocation The location where the table data will be stored
  * @param tableVersions The provider of version data.
  */
class SnapshotTableLoader(
    table: TableName,
    tableLocation: URI,
    tableVersions: TableVersions[IO],
    metastore: Metastore[IO])(implicit val spark: SparkSession)
    extends LazyLogging {

  import spark.implicits._

  def initTable(): Unit = {
    // Create table schema in metastore
    val ddl = s"""CREATE EXTERNAL TABLE IF NOT EXISTS ${table.fullyQualifiedName} (
                 |  `id` string,
                 |  `name` string,
                 |  `email` string
                 |)
                 |STORED AS parquet
                 |LOCATION '$tableLocation'
    """.stripMargin

    spark.sql(ddl)

    // Initialise version tracking for table
    tableVersions.init(table).unsafeRunSync()
  }

  def insert(dataset: Dataset[User], message: String): Unit = {

    val update: IO[TableVersion] = for {
      // Get next version to write
      newPartitionVersions <- tableVersions.nextVersions(table, PartitionSchema.snapshot.columns)
      newVersion = newPartitionVersions.head

      // Snapshot tables only use the base path of the table and the version number
      newPath = VersionPaths.pathFor(tableLocation, newVersion.version)

      // Write Spark dataset to the versioned path
      _ <- IO(dataset.write.mode(SaveMode.Overwrite).parquet(newPath.toString))

      // Commit written version
      _ <- IO(
        tableVersions.commit(
          TableUpdate(UserId("test user"), UpdateMessage(message), Instant.now(), newPartitionVersions)))

      // Get latest version details and sync the Metastore to match, effectively switching the table to the new version.
      latestVersion <- tableVersions.currentVersions(table)
      _ <- metastore.syncVersions(table, latestVersion)

    } yield latestVersion

    val latestVersion = update.unsafeRunSync()
    logger.info(s"Updated table $table, new version details:\n$latestVersion")
  }

  def users(): Dataset[User] =
    spark.table(table.fullyQualifiedName).as[User]

}

object SnapshotTableLoader {

  case class User(id: String, name: String, email: String)

}
