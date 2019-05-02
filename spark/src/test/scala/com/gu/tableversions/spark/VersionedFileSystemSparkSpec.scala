package com.gu.tableversions.spark

import com.gu.tableversions.core.Partition
import com.gu.tableversions.core.Partition.{ColumnValue, PartitionColumn}
import org.apache.spark.sql.SaveMode
import org.scalatest.{FlatSpec, Matchers}

class VersionedFileSystemSparkSpec extends FlatSpec with Matchers with SparkHiveSuite {

  spark.sparkContext.setLogLevel("ERROR")

  "VersionedFileSystem" should "write partitions with a version suffix" in {
    import spark.implicits._

    VersionedFileSystem.setUnderlyingScheme("file")
    VersionedFileSystem.setVersion("version1")

    val path = tableUri.resolve(s"table/").toString.replace("file:", "versioned://")

    VersionedFileSystem.setPartitionMappings(
      List(
        Partition(ColumnValue(PartitionColumn("date"), "2019-01-01"), ColumnValue(PartitionColumn("hour"), "01")),
        Partition(ColumnValue(PartitionColumn("date"), "2019-01-01"), ColumnValue(PartitionColumn("hour"), "02")),
        Partition(ColumnValue(PartitionColumn("date"), "2019-01-01"), ColumnValue(PartitionColumn("hour"), "03")),
        Partition(ColumnValue(PartitionColumn("date"), "2019-01-01"), ColumnValue(PartitionColumn("hour"), "04")),
        Partition(ColumnValue(PartitionColumn("date"), "2019-01-01"), ColumnValue(PartitionColumn("hour"), "05"))
      ))

    List(TestRow(1, "2019-01-01", "01"),
         TestRow(2, "2019-01-01", "02"),
         TestRow(3, "2019-01-01", "03"),
         TestRow(4, "2019-01-01", "04"),
         TestRow(5, "2019-01-01", "05")).toDS.write
      .mode(SaveMode.Append)
      .partitionBy("date", "hour")
      .parquet(path)

    val rows = spark.read.parquet(path).as[TestRow].collect.toList

    rows should contain theSameElementsAs List(
      TestRow(1, "2019-01-01", "1"),
      TestRow(2, "2019-01-01", "2"),
      TestRow(3, "2019-01-01", "3"),
      TestRow(4, "2019-01-01", "4"),
      TestRow(5, "2019-01-01", "5")
    )
  }
}

case class TestRow(value: Int, date: String, hour: String)
