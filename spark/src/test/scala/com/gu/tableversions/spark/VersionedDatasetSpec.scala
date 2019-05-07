package com.gu.tableversions.spark

import java.net.URI
import java.sql.Date
import java.time.Instant

import cats.effect.IO
import com.gu.tableversions.core.Partition.PartitionColumn
import com.gu.tableversions.core.TableVersions._
import com.gu.tableversions.core._
import com.gu.tableversions.metastore.Metastore
import com.gu.tableversions.metastore.Metastore.TableChanges
import com.gu.tableversions.metastore.Metastore.TableOperation.{AddPartition, UpdateTableVersion}
import com.gu.tableversions.spark.VersionedDataset._
import com.gu.tableversions.spark.VersionedDatasetSpec.{Event, User}
import org.apache.spark.sql.Dataset
import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.runtime.universe.TypeTag

class VersionedDatasetSpec extends FlatSpec with Matchers with SparkHiveSuite {

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  // Stub version generator that returns the same version on every invocation
  lazy val version1 = Version.generateVersion.unsafeRunSync()
  lazy val version2 = Version.generateVersion.unsafeRunSync()
  implicit val generateVersion: IO[Version] = IO.pure(version1)

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

    VersionedFileSystem.setUnderlyingScheme("file")

    val eventsGroup1 = List(
      Event("101", "A", Date.valueOf("2019-01-15")),
      Event("102", "B", Date.valueOf("2019-01-15")),
      Event("103", "A", Date.valueOf("2019-01-16")),
      Event("104", "B", Date.valueOf("2019-01-18"))
    ).toDS

    val eventsGroup2 = List(
      Event("201", "C", Date.valueOf("2019-01-15")),
      Event("202", "D", Date.valueOf("2019-01-15")),
      Event("203", "C", Date.valueOf("2019-01-16")),
      Event("204", "D", Date.valueOf("2019-01-18"))
    ).toDS()

    val partitionPathsV1: Map[Partition, Version] = Map(
      Partition(PartitionColumn("date"), "2019-01-15") -> version1,
      Partition(PartitionColumn("date"), "2019-01-16") -> version1,
      Partition(PartitionColumn("date"), "2019-01-18") -> version1
    )

    val partitionPathsV2: Map[Partition, Version] = Map(
      Partition(PartitionColumn("date"), "2019-01-15") -> version2,
      Partition(PartitionColumn("date"), "2019-01-16") -> version2,
      Partition(PartitionColumn("date"), "2019-01-18") -> version2
    )

    val table = TableDefinition(TableName("dev", "test"), tableUri, PartitionSchema(List(PartitionColumn("date"))))

    VersionedDataset.writeVersionedPartitions(eventsGroup1, table, partitionPathsV1)
    VersionedDataset.writeVersionedPartitions(eventsGroup2, table, partitionPathsV2)

    // Check that data was written to the right place.

    val path = tableUri.resolve(s"table").toString.replace("file:", "versioned://")
    readDataset[Event](new URI(path))
      .collect() should contain theSameElementsAs List(
      Event("101", "A", Date.valueOf("2019-01-15")),
      Event("102", "B", Date.valueOf("2019-01-15")),
      Event("103", "A", Date.valueOf("2019-01-16")),
      Event("104", "B", Date.valueOf("2019-01-18"))
    )

    readDataset[Event](new URI(path))
      .collect() should contain theSameElementsAs List(
      Event("201", "C", Date.valueOf("2019-01-15")),
      Event("202", "D", Date.valueOf("2019-01-15")),
      Event("203", "C", Date.valueOf("2019-01-16")),
      Event("204", "D", Date.valueOf("2019-01-18"))
    )
  }

  "Inserting a snapshot dataset" should "write the data to the versioned location and commit the new version" in {
    val usersTable = TableDefinition(TableName(schema, "users"), tableUri, PartitionSchema.snapshot)

    // Stub metastore
    val initialTableVersion = SnapshotTableVersion(Version.Unversioned)

    val stubbedChanges = TableChanges(List(UpdateTableVersion(version1)))
    implicit val stubMetastore: Metastore[IO] = new StubMetastore(
      currentVersion = initialTableVersion,
      computedChanges = stubbedChanges
    )

    implicit val tableVersions: TableVersions[IO] = (for {
      t <- InMemoryTableVersions[IO]
      _ <- t.init(usersTable.name, isSnapshot = true, UserId("test"), UpdateMessage("init"), Instant.now())
    } yield t).unsafeRunSync()

    val users = List(
      User("101", "Alice"),
      User("102", "Bob")
    )

    val timestampBeforeWriting = Instant.now()
    val userId = UserId("test-user-id")

    // The insert method that we're testing here
    val (tableVersion, metastoreChanges) =
      users.toDS().versionedInsertInto(usersTable, userId, "Test insert users into table")

    // Check that data was written correctly to the right place
    readDataset[User](resolveTablePath(version1.label)).collect() should contain theSameElementsAs users

    // Check that we performed the right version updates and returned the right results
    tableVersion shouldBe SnapshotTableVersion(version1)
    metastoreChanges shouldBe stubbedChanges

    val tableUpdates = tableVersions.updates(usersTable.name).unsafeRunSync()
    tableUpdates should have size 2
    val tableUpdate = tableUpdates.head
    tableUpdate.message shouldBe UpdateMessage("Test insert users into table")
    timestampBeforeWriting.isAfter(tableUpdate.timestamp) shouldBe false
    tableUpdate.userId shouldBe userId
  }

  "Inserting multiple records into the same partition" should "write the correct data to the filesystem" in {
    VersionedFileSystem.setUnderlyingScheme("file")

    val eventsTable =
      TableDefinition(TableName(schema, "events"), tableUri, PartitionSchema(List(PartitionColumn("date"))))

    val initialTableVersion = PartitionedTableVersion(partitionVersions = Map.empty)
    val stubbedChanges = TableChanges(initialTableVersion.partitionVersions.map(AddPartition.tupled).toList)

    // Stub metastore
    implicit val stubMetastore: Metastore[IO] = new StubMetastore(
      currentVersion = initialTableVersion,
      computedChanges = stubbedChanges
    )

    implicit val tableVersions: TableVersions[IO] = (for {
      t <- InMemoryTableVersions[IO]
      _ <- t.init(eventsTable.name, isSnapshot = false, UserId("test"), UpdateMessage("init"), Instant.now())
    } yield t).unsafeRunSync()

    val events = List(
      Event("101", "A", Date.valueOf("2019-01-15")),
      Event("102", "B", Date.valueOf("2019-01-15")),
      Event("103", "C", Date.valueOf("2019-01-15"))
    )

    val timestampBeforeWriting = Instant.now()

    // Do the insert
    val userId = UserId("user-id")
    val (tableVersion, metastoreChanges) =
      events.toDS().versionedInsertInto(eventsTable, userId, "Test insert events into table")

    // Check that data was written correctly to the right place
    readDataset[Event](resolveTablePath(""))
      .collect() should contain theSameElementsAs (events)

    // Check that we performed the right version updates and returned the right results
    val expectedPartitionVersions = Map(
      Partition(PartitionColumn("date"), "2019-01-15") -> version1
    )

    tableVersion shouldBe PartitionedTableVersion(expectedPartitionVersions)
    metastoreChanges shouldBe stubbedChanges

    val tableUpdates = tableVersions.updates(eventsTable.name).unsafeRunSync()
    tableUpdates should have size 2
    val tableUpdate = tableUpdates.head
    tableUpdate.message shouldBe UpdateMessage("Test insert events into table")
    timestampBeforeWriting.isAfter(tableUpdate.timestamp) shouldBe false
    tableUpdate.userId shouldBe userId

  }

  "Inserting a partitioned dataset" should "write the data to the versioned partitions and commit the new versions" in {

    VersionedFileSystem.setUnderlyingScheme("file")

    val eventsTable =
      TableDefinition(TableName(schema, "events"), tableUri, PartitionSchema(List(PartitionColumn("date"))))

    val initialTableVersion = PartitionedTableVersion(partitionVersions = Map.empty)
    val stubbedChanges = TableChanges(initialTableVersion.partitionVersions.map(AddPartition.tupled).toList)

    // Stub metastore
    implicit val stubMetastore: Metastore[IO] = new StubMetastore(
      currentVersion = initialTableVersion,
      computedChanges = stubbedChanges
    )

    implicit val tableVersions: TableVersions[IO] = (for {
      t <- InMemoryTableVersions[IO]
      _ <- t.init(eventsTable.name, isSnapshot = false, UserId("test"), UpdateMessage("init"), Instant.now())
    } yield t).unsafeRunSync()

    val eventsDay1 = List(
      Event("101", "A", Date.valueOf("2019-01-15")),
      Event("102", "B", Date.valueOf("2019-01-15"))
    )
    val eventsDay2 = List(
      Event("103", "A", Date.valueOf("2019-01-16"))
    )
    val eventsDay3 = List(
      Event("104", "B", Date.valueOf("2019-01-17"))
    )
    val events = eventsDay1 ++ eventsDay2 ++ eventsDay3

    val timestampBeforeWriting = Instant.now()

    // Do the insert
    val userId = UserId("user-id")
    val (tableVersion, metastoreChanges) =
      events.toDS().versionedInsertInto(eventsTable, userId, "Test insert events into table")

    // Check that data was written correctly to the right place
    readDataset[Event](resolveTablePath(""))
      .collect() should contain theSameElementsAs (eventsDay1 ++ eventsDay2 ++ eventsDay3)

    // Check that we performed the right version updates and returned the right results
    val expectedPartitionVersions = Map(
      Partition(PartitionColumn("date"), "2019-01-15") -> version1,
      Partition(PartitionColumn("date"), "2019-01-16") -> version1,
      Partition(PartitionColumn("date"), "2019-01-17") -> version1
    )
    tableVersion shouldBe PartitionedTableVersion(expectedPartitionVersions)
    metastoreChanges shouldBe stubbedChanges

    val tableUpdates = tableVersions.updates(eventsTable.name).unsafeRunSync()
    tableUpdates should have size 2
    val tableUpdate = tableUpdates.head
    tableUpdate.message shouldBe UpdateMessage("Test insert events into table")
    timestampBeforeWriting.isAfter(tableUpdate.timestamp) shouldBe false
    tableUpdate.userId shouldBe userId
  }

  private def readDataset[T <: Product: TypeTag](path: URI): Dataset[T] =
    spark.read
      .parquet(path.toString)
      .as[T]

  class StubMetastore(currentVersion: TableVersion, computedChanges: TableChanges) extends Metastore[IO] {

    override def currentVersion(table: TableName): IO[TableVersion] =
      IO(currentVersion)

    override def computeChanges(current: TableVersion, target: TableVersion): TableChanges =
      computedChanges

    override def update(table: TableName, changes: Metastore.TableChanges): IO[Unit] =
      IO.unit

  }

}

object VersionedDatasetSpec {

  case class User(id: String, name: String)

  case class Event(id: String, value: String, date: Date)

}
