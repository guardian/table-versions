package com.gu.tableversions.examples

import java.net.URI
import java.nio.file.Paths

import cats.effect.IO
import com.gu.tableversions.core.Partition.PartitionColumn
import com.gu.tableversions.core.{InMemoryTableVersions, Partition, PartitionSchema, TableName}
import com.gu.tableversions.metastore.HiveMetastore
import com.gu.tableversions.spark.SparkHiveSuite
import org.scalatest.{FlatSpec, Matchers}

class SnapshotTableLoaderSpec extends FlatSpec with Matchers with SparkHiveSuite {

  import SnapshotTableLoader._

  val tableName = TableName(schema, "identity")

  "Writing multiple versions of a snapshot dataset" should "produce distinct versions" ignore {
    import spark.implicits._

    val tableVersions = new InMemoryTableVersions[IO]()
    val metastore = new HiveMetastore[IO]()

    val tablePartitionSchema = PartitionSchema.snapshot

    val loader =
      new SnapshotTableLoader(tableName, tableUri, tablePartitionSchema, tableVersions, metastore)
    loader.initTable()

    // Write the data to the table
    val identitiesDay1 = List(
      User("user-1", "Alice", "alice@mail.com"),
      User("user-2", "Bob", "bob@mail.com"),
      User("user-3", "Carol", "carol@mail.com")
    )
    loader.insert(identitiesDay1.toDS().coalesce(2), "Committing first version from test")

    // Query the table to make sure we have the right data
    val day1TableData = loader.users().collect()
    day1TableData should contain theSameElementsAs identitiesDay1

    // Check underlying storage that the expected versions were written in the right place
    versionDirs(tableUri) shouldBe List("v1")

    // Write a slightly changed version of the table
    val identitiesDay2 = List(
      User("user-2", "Bob", "bob@mail.com"),
      User("user-3", "Carol", "carol@othermail.com"),
      User("user-4", "Dave", "dave@mail.com")
    )
    loader.insert(identitiesDay2.toDS().coalesce(2), "Committing second version from test")

    // Query it to make sure we have the right data
    val day2TableData = loader.users().collect()
    day2TableData should contain theSameElementsAs identitiesDay2

    // Check underlying storage that it was written in the right place
    versionDirs(tableUri) shouldBe List("v1", "v2")

    // Check that the previous version's data is still there and valid (not testing rollback APIs yet)
    val oldVersion = spark.read.parquet(tableUri + "/v1").as[User].collect()
    oldVersion should contain theSameElementsAs day1TableData
  }

  def versionDirs(tableLocation: URI): List[String] = {
    val dir = Paths.get(tableLocation)
    dir.toFile.list().toList.filter(_.matches("v\\d+"))
  }

}
