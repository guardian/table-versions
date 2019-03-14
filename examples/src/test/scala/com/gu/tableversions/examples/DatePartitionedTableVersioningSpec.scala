package com.gu.tableversions.examples

import java.net.URI
import java.nio.file.Paths
import java.sql.{Date, Timestamp}
import java.time.{ZoneId, ZonedDateTime}

import com.gu.tableversions.spark.SparkHiveSuite
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.runtime.universe.TypeTag

class DatePartitionedTableVersioningSpec extends FlatSpec with Matchers with SparkHiveSuite {

  import DatePartitionedTableVersioningSpec._

  val tableName = s"$schema.pageview"

  "Writing multiple versions of a date partitioned datatset" should "produce distinct versions" in {

    import spark.implicits._

    initTable(tableName, tableUri)

    val pageviewsDay1 = List(
      Pageview("user-1", "news/politics", Timestamp.valueOf("2019-03-13 00:20:00")),
      Pageview("user-1", "news/uk", Timestamp.valueOf("2019-03-13 00:20:10")),
      Pageview("user-2", "news/politics", Timestamp.valueOf("2019-03-13 00:10:00")),
      Pageview("user-3", "sport/football", Timestamp.valueOf("2019-03-13 21:00:00"))
    )

    writeVersionedDataset(tableName, pageviewsDay1.toDS().coalesce(2))

    val x = spark.table(tableName).as[Pageview].collect()
    spark.table(tableName).as[Pageview].collect() should contain theSameElementsAs pageviewsDay1

    val pageviewsDay2 = List(
      Pageview("user-2", "news/politics", Timestamp.valueOf("2019-03-14 13:00:00")),
      Pageview("user-3", "sport/handball", Timestamp.valueOf("2019-03-14 21:00:00")),
      Pageview("user-4", "business", Timestamp.valueOf("2019-03-14 14:00:00"))
    )

    writeVersionedDataset(tableName, pageviewsDay2.toDS().coalesce(2))
    spark.table(tableName).as[Pageview].collect() should contain theSameElementsAs pageviewsDay1 ++ pageviewsDay2

    val pageviewsDay3 = List(
      Pageview("user-1", "news/politics", Timestamp.valueOf("2019-03-15 00:20:00")),
      Pageview("user-2", "news/politics/budget", Timestamp.valueOf("2019-03-15 00:10:00")),
      Pageview("user-3", "sport/football", Timestamp.valueOf("2019-03-15 21:00:00"))
    )

    writeVersionedDataset(tableName, pageviewsDay3.toDS().coalesce(2))

    val initialPageviews = spark.table(tableName).as[Pageview]

    initialPageviews.collect() should contain theSameElementsAs pageviewsDay1 ++ pageviewsDay2 ++ pageviewsDay3

    // Check that data was written to the right partitions
    spark
      .table(tableName)
      .as[Pageview]
      .where('date === "2019-03-13")
      .collect() should contain theSameElementsAs pageviewsDay1

    spark
      .table(tableName)
      .as[Pageview]
      .where('date === "2019-03-14")
      .collect() should contain theSameElementsAs pageviewsDay2

    spark
      .table(tableName)
      .as[Pageview]
      .where('date === "2019-03-15")
      .collect() should contain theSameElementsAs pageviewsDay3

    versionDirs(tableUri, "2019-03-13") shouldBe List("v1")
    versionDirs(tableUri, "2019-03-14") shouldBe List("v1")
    versionDirs(tableUri, "2019-03-15") shouldBe List("v1")

    // Rewrite pageviews to remove one of the identity IDs, affecting only day 2
    val updatedPageviewsDay2 = pageviewsDay2.filter(_.id != "user-4")
    writeVersionedDataset(tableName, updatedPageviewsDay2.toDS().coalesce(2))

    // Query to check we see the updated data
    val updatedPageviews = spark.table(tableName).as[Pageview]
    updatedPageviews.collect() should contain theSameElementsAs pageviewsDay1 ++ updatedPageviewsDay2 ++ pageviewsDay3

    // Check underlying storage that we have both versions for the updated partition
    versionDirs(tableUri, "2019-03-13") shouldBe List("v1")
    versionDirs(tableUri, "2019-03-14") shouldBe List("v1", "v2")
    versionDirs(tableUri, "2019-03-15") shouldBe List("v1")

    // TODO: Query Metastore directly to check what partitions the table has?
    //   (When implementing rollback and creating views on historical versions we could just test that functionality
    //    instead of querying storage directly)

    // Check the data in the original partition version is intact
    val pageviewsDay2OldVersion = spark.read.parquet(tableUri + "2019-03-14/v1").as[Pageview].collect()
    pageviewsDay2OldVersion should contain theSameElementsAs pageviewsDay2
  }

  def versionDirs(folderUri: String, partition: String): List[String] = {
    val dir = Paths.get(new URI(folderUri))
    dir.toFile.list().toList.filter(_.matches("v\\d+"))
  }

  def writeVersionedDataset[A: TypeTag](fullyQualifiedTableName: String, dataset: Dataset[A])(
      implicit spark: SparkSession): Unit = {
    // TODO: This just writes directly now - clearly this isn't going to work!
    dataset.write
      .mode(SaveMode.Overwrite)
      .insertInto(fullyQualifiedTableName)

    spark.sql(s"REFRESH TABLE $fullyQualifiedTableName")
    ()
  }

  def initTable(fullyQualifiedTableName: String, locationUri: String): Unit = {
    val ddl = s"""CREATE EXTERNAL TABLE IF NOT EXISTS $fullyQualifiedTableName (
                 |  `id` string,
                 |  `path` string,
                 |  `timestamp` string
                 |)
                 |PARTITIONED BY (`date` date)
                 |STORED AS parquet
                 |LOCATION '$locationUri'
    """.stripMargin

    spark.sql(ddl)
    ()
  }

}

object DatePartitionedTableVersioningSpec {

  case class Pageview(id: String, path: String, timestamp: Timestamp, date: Date)

  object Pageview {

    def apply(id: String, path: String, timestamp: Timestamp): Pageview =
      Pageview(id, path, timestamp, timestampToUtcDate(timestamp))

    private def timestampToUtcDate(timestamp: Timestamp): Date = {
      val zoneId = ZoneId.of("UTC")
      val zonedDateTime = ZonedDateTime.ofInstant(timestamp.toInstant, zoneId)
      java.sql.Date.valueOf(zonedDateTime.toLocalDate)
    }

  }

}
