package com.gu.tableversions.metastore

import cats.effect.IO
import com.gu.tableversions.core.Partition.{ColumnValue, PartitionColumn}
import com.gu.tableversions.core._
import com.gu.tableversions.metastore.SparkHiveMetastore.parseVersion
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class SparkHiveMetastoreSpec extends FlatSpec with Matchers with SparkHiveSuite with MetastoreSpec with PropertyChecks {

  val snapshotTable =
    TableDefinition(TableName(schema, "users"), resolveTablePath("users"), PartitionSchema.snapshot)

  val partitionedTable = TableDefinition(TableName(schema, "clicks"),
                                         resolveTablePath("clicks"),
                                         PartitionSchema(List(PartitionColumn("date"))))

  "A metastore implementation" should behave like snapshotTableBehaviour(new SparkHiveMetastore, snapshotTable)

  it should behave like partitionedTableBehaviour(new SparkHiveMetastore, partitionedTable)

  "Adding another test" should "foo" in {
    2 + 2 shouldBe 4
  }

  "Parsing a valid partition string" should "produce the expected values" in {
    val testData = Table(
      ("partitionStr", "expected"),
      ("date=2019-01-31", Partition(List(ColumnValue(PartitionColumn("date"), "2019-01-31")))),
      ("event_date=2019-01-30/processed_date=2019-01-31",
       Partition(
         List(ColumnValue(PartitionColumn("event_date"), "2019-01-30"),
              ColumnValue(PartitionColumn("processed_date"), "2019-01-31")))),
      ("year=2019/month=01/day=31",
       Partition(
         List(ColumnValue(PartitionColumn("year"), "2019"),
              ColumnValue(PartitionColumn("month"), "01"),
              ColumnValue(PartitionColumn("day"), "31"))))
    )

    forAll(testData) { (partitionStr: String, expected: Partition) =>
      SparkHiveMetastore.parsePartition(partitionStr) shouldBe expected
    }
  }

  it should "do what for invalid partition strings?" in {
    // format: off
    val invalidPartitionStrings =
      Table(
        "partitionString",
        "invalid partition string",
        "invalid partition string=42",
        //"/",
        "")
    // format: on

    forAll(invalidPartitionStrings) { partitionStr =>
      an[Exception] should be thrownBy SparkHiveMetastore.parsePartition(partitionStr)
    }
  }

  "Rendering a partition as a string" should "produce a valid Hive partition expression" in {

    val testData = Table(
      ("partition", "expected partition expression"),
      Partition(
        List(ColumnValue(PartitionColumn("event_date"), "2019-01-30"),
             ColumnValue(PartitionColumn("processed_date"), "2019-01-31"))) -> "(event_date='2019-01-30',processed_date='2019-01-31')",
      Partition(List(ColumnValue(PartitionColumn("date"), "2019-01-31"))) -> "(date='2019-01-31')"
    )

    forAll(testData) { (partition, expectedExpr) =>
      SparkHiveMetastore.toHivePartitionExpr(partition) shouldBe expectedExpr
    }

  }

  "Parsing the version from a versioned path" should "produce the version number" in {
    parseVersion("file:/tmp/7bbc577c-471d-4ece-8462-36147c30bdf3/table/date=2019-01-21/v5") shouldBe VersionNumber(5)
    parseVersion("s3://bucket/pageview/date=2019-01-21/v5") shouldBe VersionNumber(5)
  }

  "Parsing the version from an unversioned path" should "produce version 0" in {
    parseVersion("s3://bucket/pageview/date=2019-01-21") shouldBe VersionNumber(0)
  }

  "Converting a partition path to a Hive partition expression" should "do the expected conversion" in {
    val testData = Table(
      ("partition path", "expected Hive partition expression"),
      ("date=2019-01-30", "(date='2019-01-30')"),
      ("event_date=2019-01-30/processed_date=2019-01-31", "(event_date='2019-01-30',processed_date='2019-01-31')")
    )

    forAll(testData) { (partitionPath, expected) =>
      SparkHiveMetastore.toPartitionExpr(partitionPath) shouldBe expected
    }
  }

}
