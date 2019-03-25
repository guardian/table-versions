package com.gu.tableversions.metastore

import cats.effect.IO
import com.gu.tableversions.core.Partition.PartitionColumn
import com.gu.tableversions.core._
import com.gu.tableversions.metastore.Metastore.TableChanges
import com.gu.tableversions.metastore.Metastore.TableOperation._
import org.scalatest.{FlatSpec, Matchers}

trait MetastoreSpec {
  this: FlatSpec with Matchers =>

  def snapshotTableBehaviour(emptyMetastore: => Metastore[IO], table: TableDefinition): Unit = {

    it should "allow table versions to be updated for snapshot tables" in {

      val metastore: Metastore[IO] = emptyMetastore

      val scenario = for {
        // Get version before any updates
        initialVersion <- metastore.currentVersion(table.name)

        _ <- metastore.update(table.name, TableChanges(List(UpdateTableLocation(table.location, VersionNumber(1)))))

        firstUpdatedVersion <- metastore.currentVersion(table.name)

        _ <- metastore.update(table.name, TableChanges(List(UpdateTableLocation(table.location, VersionNumber(42)))))

        secondUpdatedVersion <- metastore.currentVersion(table.name)

        _ <- metastore.update(table.name, TableChanges(List(UpdateTableLocation(table.location, VersionNumber(1)))))

        revertedVersion <- metastore.currentVersion(table.name)

      } yield (initialVersion, firstUpdatedVersion, secondUpdatedVersion, revertedVersion)

      val (initialVersion, firstUpdatedVersion, secondUpdatedVersion, revertedVersion) = scenario.unsafeRunSync()

      initialVersion shouldBe None
      firstUpdatedVersion shouldBe Some(
        TableVersion(List(PartitionVersion(Partition.snapshotPartition, VersionNumber(1)))))
      secondUpdatedVersion shouldBe Some(
        TableVersion(List(PartitionVersion(Partition.snapshotPartition, VersionNumber(42)))))
      revertedVersion shouldBe firstUpdatedVersion
    }

  }

  def partitionedTableBehaviour(emptyMetastore: => Metastore[IO], table: TableDefinition): Unit = {

    val dateCol = PartitionColumn("date")

    it should "allow individual partitions to be updated in partitioned tables" in {
      val metastore: Metastore[IO] = emptyMetastore

      val scenario = for {
        initialVersion <- metastore.currentVersion(table.name)

        _ <- metastore.update(
          table.name,
          TableChanges(
            List(
              AddPartition(PartitionVersion(Partition(dateCol, "2019-03-01"), VersionNumber(0))),
              AddPartition(PartitionVersion(Partition(dateCol, "2019-03-02"), VersionNumber(1))),
              AddPartition(PartitionVersion(Partition(dateCol, "2019-03-03"), VersionNumber(1)))
            )
          )
        )

        versionAfterFirstUpdate <- metastore.currentVersion(table.name)

        _ <- metastore.update(
          table.name,
          TableChanges(
            List(
              UpdatePartitionVersion(PartitionVersion(Partition(dateCol, "2019-03-01"), VersionNumber(1))),
              UpdatePartitionVersion(PartitionVersion(Partition(dateCol, "2019-03-03"), VersionNumber(2)))
            )
          )
        )

        versionAfterSecondUpdate <- metastore.currentVersion(table.name)

        _ <- metastore.update(
          table.name,
          TableChanges(
            List(
              RemovePartition(Partition(dateCol, "2019-03-01"))
            )
          )
        )

        versionAfterPartitionRemoved <- metastore.currentVersion(table.name)

      } yield (initialVersion, versionAfterFirstUpdate, versionAfterSecondUpdate, versionAfterPartitionRemoved)

      val (initialVersion, versionAfterFirstUpdate, versionAfterSecondUpdate, versionAfterPartitionRemoved) =
        scenario.unsafeRunSync()

      initialVersion shouldBe None

      versionAfterFirstUpdate shouldBe Some(
        TableVersion(List(
          PartitionVersion(Partition(dateCol, "2019-03-01"), VersionNumber(0)),
          PartitionVersion(Partition(dateCol, "2019-03-02"), VersionNumber(1)),
          PartitionVersion(Partition(dateCol, "2019-03-03"), VersionNumber(1))
        )))

      versionAfterSecondUpdate shouldBe Some(
        TableVersion(List(
          PartitionVersion(Partition(dateCol, "2019-03-01"), VersionNumber(1)),
          PartitionVersion(Partition(dateCol, "2019-03-02"), VersionNumber(1)),
          PartitionVersion(Partition(dateCol, "2019-03-03"), VersionNumber(2))
        )))

      versionAfterPartitionRemoved shouldBe Some(
        TableVersion(
          List(
            PartitionVersion(Partition(dateCol, "2019-03-01"), VersionNumber(1)),
            PartitionVersion(Partition(dateCol, "2019-03-03"), VersionNumber(2))
          ))
      )
    }

    it should "not allow updating the version of an unknown partition" ignore {
      fail("TODO")
    }

  }
}
