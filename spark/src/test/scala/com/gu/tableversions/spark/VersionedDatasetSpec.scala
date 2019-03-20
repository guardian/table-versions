package com.gu.tableversions.spark

import java.net.URI
import java.sql.Date

import com.gu.tableversions.core.Partition.PartitionColumn
import com.gu.tableversions.core.{Partition, PartitionSchema}
import com.gu.tableversions.spark.VersionedDatasetSpec.{Event, User}
import org.apache.spark.sql.Dataset
import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.runtime.universe.TypeTag

class VersionedDatasetSpec extends FlatSpec with Matchers with SparkHiveSuite {

  import spark.implicits._

  "Finding the partitions of a dataset" should "return the empty partition for an un-partitioned dataset" in {

    val snapshotDataset: Dataset[User] = List(
      User("101", "Alice"),
      User("102", "Bob")
    ).toDS()

    val schema = PartitionSchema.snapshot

    VersionedDataset.partitionValues(snapshotDataset, schema) shouldBe List(Partition.snapshotPartition)
  }

  it should "return all partitions for a dataset with a single partition column" in {

    val partitionedDataset: Dataset[Event] = List(
      Event("101", "A", Date.valueOf("2019-01-15")),
      Event("102", "B", Date.valueOf("2019-01-15")),
      Event("103", "A", Date.valueOf("2019-01-16")),
      Event("104", "B", Date.valueOf("2019-01-18"))
    ).toDS()

    val schema = PartitionSchema(List(PartitionColumn("date")))

    val expectedPartitions = List(
      Partition(PartitionColumn("date"), "2019-01-15"),
      Partition(PartitionColumn("date"), "2019-01-16"),
      Partition(PartitionColumn("date"), "2019-01-18")
    )
    VersionedDataset.partitionValues(partitionedDataset, schema) should contain theSameElementsAs expectedPartitions
  }

  it should "return no partitions for an empty dataset with a partitioned schema" in {
    val schema = PartitionSchema(List(PartitionColumn("date")))
    VersionedDataset.partitionValues(spark.emptyDataset[Event], schema) shouldBe empty
  }

  "Writing a dataset with multiple partitions" should "store the data for each partition in a versioned folder for the partition" in {

    val dataset: Dataset[Event] = List(
      Event("101", "A", Date.valueOf("2019-01-15")),
      Event("102", "B", Date.valueOf("2019-01-15")),
      Event("103", "A", Date.valueOf("2019-01-16")),
      Event("104", "B", Date.valueOf("2019-01-18"))
    ).toDS()

    val partitionPaths: Map[Partition, URI] = Map(
      Partition(PartitionColumn("date"), "2019-01-15") -> tableUri.resolve("data/date=2019-01-15/v5"),
      Partition(PartitionColumn("date"), "2019-01-16") -> tableUri.resolve("data/date=2019-01-16/v1"),
      Partition(PartitionColumn("date"), "2019-01-18") -> tableUri.resolve("data/date=2019-01-18/v3")
    )

    VersionedDataset.writeVersionedPartitions(dataset, partitionPaths).unsafeRunSync()

    // Check that data was written to the right place.

    readDataset[Event](tableUri.resolve("data/date=2019-01-15/v5")).collect() should contain theSameElementsAs List(
      Event("101", "A", Date.valueOf("2019-01-15")),
      Event("102", "B", Date.valueOf("2019-01-15"))
    )

    readDataset[Event](tableUri.resolve("data/date=2019-01-16/v1")).collect() should contain theSameElementsAs List(
      Event("103", "A", Date.valueOf("2019-01-16"))
    )

    readDataset[Event](tableUri.resolve("data/date=2019-01-18/v3")).collect() should contain theSameElementsAs List(
      Event("104", "B", Date.valueOf("2019-01-18"))
    )
  }

  private def readDataset[T <: Product: TypeTag](path: URI): Dataset[T] =
    spark.read
      .parquet(path.toString)
      .as[T]

}

object VersionedDatasetSpec {

  case class User(id: String, name: String)

  case class Event(id: String, value: String, date: Date)

}
