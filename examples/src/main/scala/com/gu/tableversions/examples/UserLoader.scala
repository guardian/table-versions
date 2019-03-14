package com.gu.tableversions.examples

import com.gu.tableversions.examples.UserLoader.User
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
  * This is an example of loading data into a 'snapshot' table, that is, a table where we replace all the content
  * every time we write to it (no partial updates).
  *
  * @param tableName The fully qualified table name
  * @param tableUri The location where the table data will be stored
  */
class UserLoader(tableName: String, tableUri: String)(implicit val spark: SparkSession) {

  import spark.implicits._

  def initTable(): Unit = {
    val ddl = s"""CREATE EXTERNAL TABLE IF NOT EXISTS $tableName (
                 |  `id` string,
                 |  `name` string,
                 |  `email` string
                 |)
                 |STORED AS parquet
                 |LOCATION '$tableUri'
    """.stripMargin

    spark.sql(ddl)
    ()
  }

  def insert(dataset: Dataset[User])(implicit spark: SparkSession): Unit = {
    // Currently, this just uses the basic implementation of writing data to tables via Hive.
    // This will not do any versioning as-is - this is the implementation we need to replace
    // with new functionality in this library.
    dataset.write.mode(SaveMode.Overwrite).parquet(tableUri)
    spark.sql(s"REFRESH TABLE $tableName")
    ()
  }

  def users(): Dataset[User] =
    spark.table(tableName).as[User]

}

object UserLoader {

  case class User(id: String, name: String, email: String)

}
