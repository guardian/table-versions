package com.gu.tableversions.core

import java.time.Instant

import cats.effect.IO
import com.gu.tableversions.core.Partition.PartitionColumn
import com.gu.tableversions.core.TableVersions.CommitResult.SuccessfulCommit
import com.gu.tableversions.core.TableVersions._
import org.scalatest.{FlatSpec, Matchers}

/**
  * Spec containing tests that apply across all TableVersions implementations.
  *
  * These are black box tests purely in terms of the TableVersions interface.
  */
trait TableVersionsSpec {
  this: FlatSpec with Matchers =>

  def tableVersionsBehaviour(emptyTableVersions: IO[TableVersions[IO]]): Unit = {

    val table = TableName("schema", "table")
    val userId = UserId("Test user")
    val date = PartitionColumn("date")

    it should "have an idempotent 'init' operation" in {

      val scenario = for {
        tableVersions <- emptyTableVersions
        _ <- tableVersions.init(table)
        _ <- tableVersions.init(table)
        _ <- tableVersions.init(table)

        tableVersion <- tableVersions.currentVersion(table)

      } yield tableVersion

      val tableVersion = scenario.unsafeRunSync()

      tableVersion shouldBe TableVersion.empty
    }

    it should "allow partition versions of a partitioned table to be updated and queried" in {

      val initialPartitionVersions = List(
        PartitionVersion(Partition(date, "2019-03-01"), VersionNumber(1)),
        PartitionVersion(Partition(date, "2019-03-02"), VersionNumber(1))
      )

      val partitionUpdate1 = List(
        PartitionVersion(Partition(date, "2019-03-02"), VersionNumber(2)),
        PartitionVersion(Partition(date, "2019-03-03"), VersionNumber(1))
      )

      val scenario = for {
        tableVersions <- emptyTableVersions
        _ <- tableVersions.init(table)

        initialTableVersion <- tableVersions.currentVersion(table)

        // Add some partitions
        commitResult1 <- tableVersions.commit(
          table,
          TableUpdate(userId,
                      UpdateMessage("Add initial partitions"),
                      timestamp(1),
                      initialPartitionVersions.map(AddPartitionVersion))
        )

        version1 <- tableVersions.currentVersion(table)

        // Do an update with one updated partition and one new one
        nextVersions1 <- tableVersions.nextVersions(table, partitionUpdate1.map(_.partition))
        commitResult2 <- tableVersions.commit(
          table,
          TableUpdate(userId, UpdateMessage("First update"), timestamp(2), partitionUpdate1.map(AddPartitionVersion)))
        version2 <- tableVersions.currentVersion(table)

      } yield (initialTableVersion, commitResult1, version1, nextVersions1, commitResult2, version2)

      val (initialTableVersion, commitResult1, version1, nextVersions1, commitResult2, version2) =
        scenario.unsafeRunSync()

      initialTableVersion shouldBe TableVersion.empty
      commitResult1 shouldBe SuccessfulCommit
      version1 shouldBe TableVersion(initialPartitionVersions)

      nextVersions1 shouldBe partitionUpdate1
      commitResult2 shouldBe SuccessfulCommit
      version2.partitionVersions should contain theSameElementsAs List(
        PartitionVersion(Partition(date, "2019-03-01"), VersionNumber(1)),
        PartitionVersion(Partition(date, "2019-03-02"), VersionNumber(2)),
        PartitionVersion(Partition(date, "2019-03-03"), VersionNumber(1))
      )

      // TODO: Remove a partition
    }

    it should "allow versions of a snapshot table to be updated and queried" in {

      val version1 = TableVersion.snapshotVersion(VersionNumber(1))
      val version2 = TableVersion.snapshotVersion(VersionNumber(2))

      val scenario = for {
        tableVersions <- emptyTableVersions
        _ <- tableVersions.init(table)
        initialTableVersion <- tableVersions.currentVersion(table)

        nextVersion1 <- tableVersions.nextVersions(table, List(Partition.snapshotPartition))
        commitResult1 <- tableVersions.commit(table,
                                              TableUpdate(userId,
                                                          UpdateMessage("First commit"),
                                                          timestamp(1),
                                                          version1.partitionVersions.map(AddPartitionVersion)))
        currentVersion1 <- tableVersions.currentVersion(table)

        nextVersion2 <- tableVersions.nextVersions(table, List(Partition.snapshotPartition))
        commitResult2 <- tableVersions.commit(table,
                                              TableUpdate(userId,
                                                          UpdateMessage("Second commit"),
                                                          timestamp(2),
                                                          version1.partitionVersions.map(AddPartitionVersion)))
        currentVersion2 <- tableVersions.currentVersion(table)

      } yield
        (initialTableVersion,
         nextVersion1,
         commitResult1,
         currentVersion1,
         nextVersion2,
         commitResult2,
         currentVersion2)

      val (initialTableVersion,
           nextVersion1,
           commitResult1,
           currentVersion1,
           nextVersion2,
           commitResult2,
           currentVersion2) =
        scenario.unsafeRunSync()

      initialTableVersion shouldBe TableVersion.empty
      nextVersion1 shouldBe version1.partitionVersions

      commitResult1 shouldBe SuccessfulCommit
      currentVersion1 shouldBe version1

      nextVersion2 shouldBe version2.partitionVersions
      commitResult2 shouldBe SuccessfulCommit
      currentVersion2 shouldBe currentVersion2
    }

    it should "return an error if trying to get current version of an unknown table" in {
      val scenario = for {
        tableVersions <- emptyTableVersions
        version <- tableVersions.currentVersion(table)
      } yield version

      val ex = the[Exception] thrownBy scenario.unsafeRunSync()
      ex.getMessage should include regex "schema.*table.*not found"
    }

    it should "return an error if trying to get next versions from an unknown table" in {
      val scenario = for {
        tableVersions <- emptyTableVersions
        version <- tableVersions.nextVersions(TableName("schema", "table"), List(Partition.snapshotPartition))
      } yield version

      val ex = the[Exception] thrownBy scenario.unsafeRunSync()
      ex.getMessage should include regex "schema.*table.*not found"
    }

    it should "return an error if trying to commit changes for an unknown table" in {
      val scenario = for {
        tableVersions <- emptyTableVersions

        version <- tableVersions.commit(
          TableName("schema", "table"),
          TableUpdate(userId,
                      UpdateMessage("Commit initial partitions"),
                      timestamp(1),
                      List(AddPartitionVersion(PartitionVersion.snapshot(VersionNumber(1)))))
        )
      } yield version

      val ex = the[Exception] thrownBy scenario.unsafeRunSync()
      ex.getMessage should include regex "Unknown table.*schema.*table"
    }

  }

  private def timestamp(tick: Long): Instant = Instant.ofEpochSecond(1553705295L + (tick * 60))

}
