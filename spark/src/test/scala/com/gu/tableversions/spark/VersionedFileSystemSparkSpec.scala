package com.gu.tableversions.spark

import com.gu.tableversions.core.Version
import org.apache.spark.sql.SaveMode
import org.scalatest.{FlatSpec, Matchers}

class VersionedFileSystemSparkSpec extends FlatSpec with Matchers with SparkHiveSuite {

  spark.sparkContext.setLogLevel("ERROR")

  override def customConfig: Map[String, String] =
    VersionedFileSystem.sparkConfig("file", Version.generateVersion.unsafeRunSync())

  "VersionedFileSystem" should "write partitions with a version suffix" ignore { // TODO: make it pass!
    import spark.implicits._

    val path = tableUri.resolve(s"table/").toString.replace("file:", "versioned://")

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
