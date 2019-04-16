package com.gu.tableversions.core

import cats.effect.IO
import com.gu.tableversions.core.Partition.PartitionColumn
import com.gu.tableversions.core.TableVersions.PartitionOperation._
import org.scalatest.{FlatSpec, Matchers}

class InMemoryTableVersionsSpec extends FlatSpec with Matchers with TableVersionsSpec {

  val date = PartitionColumn("date")

  "The reference implementation for the TableVersions service" should behave like tableVersionsBehaviour {
    InMemoryTableVersions[IO]
  }

  "Combining partition operations" should "produce an empty table version when no updates have been applied" in {
    InMemoryTableVersions.applyUpdate(TableVersion.empty)(Nil) shouldBe TableVersion.empty
  }

  it should "produce the same table when an empty update is applied" in {
    val partitionVersions = Map(
      Partition(date, "2019-03-01") -> Version(3),
      Partition(date, "2019-03-02") -> Version(1)
    )
    val tableVersion = TableVersion(partitionVersions)
    InMemoryTableVersions.applyUpdate(tableVersion)(Nil) shouldBe tableVersion
  }

  it should "produce a version with the given partitions when no previous partition versions exist" in {
    val partitionVersions = Map(
      Partition(date, "2019-03-01") -> Version(3),
      Partition(date, "2019-03-02") -> Version(1)
    )
    val partitionUpdates = partitionVersions.map(AddPartitionVersion.tupled).toList
    InMemoryTableVersions.applyUpdate(TableVersion.empty)(partitionUpdates) shouldBe TableVersion(partitionVersions)
  }

  it should "pick the latest version when an existing partition version is updated" in {

    val initialPartitionVersions = Map(
      Partition(date, "2019-03-01") -> Version(3),
      Partition(date, "2019-03-02") -> Version(2),
      Partition(date, "2019-03-03") -> Version(1)
    )
    val initialTableVersion = TableVersion(initialPartitionVersions)

    val partitionUpdates = List(AddPartitionVersion(Partition(date, "2019-03-02"), Version(3)))

    val expectedPartitionVersions = Map(
      Partition(date, "2019-03-01") -> Version(3),
      Partition(date, "2019-03-02") -> Version(3),
      Partition(date, "2019-03-03") -> Version(1)
    )

    InMemoryTableVersions.applyUpdate(initialTableVersion)(partitionUpdates) shouldBe TableVersion(
      expectedPartitionVersions)
  }

  it should "remove an existing partition" in {
    val initialPartitionVersions = Map(
      Partition(date, "2019-03-01") -> Version(3),
      Partition(date, "2019-03-02") -> Version(2),
      Partition(date, "2019-03-03") -> Version(1)
    )
    val initialTableVersion = TableVersion(initialPartitionVersions)

    val partitionUpdates = List(RemovePartition(Partition(date, "2019-03-02")))

    val expectedPartitionVersions = Map(
      Partition(date, "2019-03-01") -> Version(3),
      Partition(date, "2019-03-03") -> Version(1)
    )

    InMemoryTableVersions.applyUpdate(initialTableVersion)(partitionUpdates) shouldBe TableVersion(
      expectedPartitionVersions)
  }

}
