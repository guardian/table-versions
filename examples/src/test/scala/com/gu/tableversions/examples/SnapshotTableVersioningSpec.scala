package com.gu.tableversions.examples

import java.net.URI
import java.nio.file.Paths

import com.gu.tableversions.spark.SparkHiveSuite
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.runtime.universe.TypeTag

class SnapshotTableVersioningSpec extends FlatSpec with Matchers with SparkHiveSuite {

  import SnapshotTableVersioningSpec._

  val tableName = s"$schema.identity"

  "Writing multiple versions of a snapshot dataset" should "produce distinct versions" in {
    import spark.implicits._

    // Given some table data
    val identitiesDay1 = List(
      Identity("user-1", "Alice", "alice@mail.com"),
      Identity("user-2", "Bob", "bob@mail.com"),
      Identity("user-3", "Carol", "carol@mail.com")
    )

    // Create a table for it
    initTable(tableName, tableUri)

    // Write the data to the table
    writeVersionedDataset(tableName, tableUri, identitiesDay1.toDS().coalesce(2))

    // Query the table to make sure we have the right data
    val day1TableData = spark.table(tableName).as[Identity].collect()
    day1TableData should contain theSameElementsAs identitiesDay1

    // Check underlying storage that it was written in the right place
    versionDirs(tableUri) shouldBe List("v1")

    // Write a slightly changed version of the table
    val identitiesDay2 = List(
      Identity("user-2", "Bob", "bob@mail.com"),
      Identity("user-3", "Carol", "carol@othermail.com"),
      Identity("user-4", "Dave", "dave@mail.com")
    )
    writeVersionedDataset(tableName, tableUri, identitiesDay2.toDS().coalesce(2))

    // Query it to make sure we have the right data
    val day2TableData = spark.table(tableName).as[Identity].collect()
    day2TableData should contain theSameElementsAs identitiesDay2

    // Check underlying storage that it was written in the right place
    versionDirs(tableUri) shouldBe List("v1", "v2")

    // Check that the previous version's data is still there and valid (not testing rollback APIs yet)
    val oldVersion = spark.read.parquet(tableUri + "/v1").as[Identity].collect()
    oldVersion should contain theSameElementsAs day1TableData
  }

  def initTable(fullyQualifiedTableName: String, locationUri: String): Unit = {
    val ddl = s"""CREATE EXTERNAL TABLE IF NOT EXISTS $fullyQualifiedTableName (
                  |  `id` string,
                  |  `name` string,
                  |  `email` string
                  |)
                  |STORED AS parquet
                  |LOCATION '$locationUri'
    """.stripMargin

    spark.sql(ddl)
    ()
  }

  def versionDirs(folderUri: String): List[String] = {
    val dir = Paths.get(new URI(folderUri))
    dir.toFile.list().toList.filter(_.matches("v\\d+"))
  }

  def writeVersionedDataset[A: TypeTag](fullyQualifiedTableName: String, tableUri: String, dataset: Dataset[A])(
      implicit spark: SparkSession): Unit = {
    // TODO: This just writes directly now - clearly this isn't going to work!
    dataset.write.mode(SaveMode.Overwrite).parquet(tableUri)
    spark.sql(s"REFRESH TABLE $fullyQualifiedTableName")
    ()
  }

}

object SnapshotTableVersioningSpec {

  case class Identity(id: String, name: String, email: String)

}
