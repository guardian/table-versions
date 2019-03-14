package com.gu.tableversions.examples

import java.sql.{Date, Timestamp}
import java.time.{ZoneId, ZonedDateTime}

import com.gu.tableversions.examples.PageviewLoader.Pageview
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
  * This example contains code that writes example event data to a table that has a single date partition.
  * It demonstrates how individual partitions in such a table can be updated using versioning.
  *
  * @param tableName the fully qualified table name that will be populated by this loader
  * @param tableUri The location where the table data will be stored
  */
class PageviewLoader(tableName: String, tableUri: String)(implicit val spark: SparkSession) {

  import spark.implicits._

  def initTable(): Unit = {
    val ddl = s"""CREATE EXTERNAL TABLE IF NOT EXISTS $tableName (
                 |  `id` string,
                 |  `path` string,
                 |  `timestamp` string
                 |)
                 |PARTITIONED BY (`date` date)
                 |STORED AS parquet
                 |LOCATION '$tableUri'
    """.stripMargin

    spark.sql(ddl)
    ()
  }

  def pageviews(): Dataset[Pageview] =
    spark.table(tableName).as[Pageview]

  def insert(dataset: Dataset[Pageview])(implicit spark: SparkSession): Unit = {
    // Currently, this just uses the basic implementation of writing data to tables via Hive.
    // This will not do any versioning as-is - this is the implementation we need to replace
    // with new functionality in this library.
    dataset.write
      .mode(SaveMode.Overwrite)
      .insertInto(tableName)
  }

}

object PageviewLoader {

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
