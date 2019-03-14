package com.gu.tableversions.examples

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.SparkSession

/**
  * This example contains code that writes example event data to a table with multiple partition columns.
  * It demonstrates how individual partitions in such a table can be updated using versioning.
  *
  * @param tableName the fully qualified table name that will be populated by this loader
  * @param tableUri The location where the table data will be stored
  */
class AdImpressionLoader(tableName: String, tableUri: String)(implicit val spark: SparkSession) {

  // TODO!

}

object AdImpressionLoader {

  case class AdImpression(
      id: String,
      impressionTimestamp: Timestamp,
      eventTimestamp: Timestamp,
      impressionDate: Date,
      eventDate: Date)

}
