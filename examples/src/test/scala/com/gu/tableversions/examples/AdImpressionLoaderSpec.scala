package com.gu.tableversions.examples

import java.nio.file.Path
import java.sql.{Date, Timestamp}

import com.gu.tableversions.examples.AdImpressionLoader.AdImpression
import com.gu.tableversions.spark.SparkHiveSuite
import org.scalatest.{FlatSpec, Matchers}

class AdImpressionLoaderSpec extends FlatSpec with Matchers with SparkHiveSuite {

  "Writing multiple versions of a snapshot dataset" should "produce distinct versions" in {

    import spark.implicits._

    val loader = new AdImpressionLoader(s"$schema.ad_impressions", tableUri)
    loader.initTable()

    val impressionsDay1 = List(
      AdImpression("user-1", "ad-1", Timestamp.valueOf("2019-03-13 23:59:00"), Date.valueOf("2019-03-14")),
      AdImpression("user-2", "ad-1", Timestamp.valueOf("2019-03-14 00:00:10"), Date.valueOf("2019-03-14")),
      AdImpression("user-3", "ad-2", Timestamp.valueOf("2019-03-14 00:00:10"), Date.valueOf("2019-03-14"))
    )
    loader.insert(impressionsDay1.toDS().coalesce(2))
    loader.adImpressions().collect() should contain theSameElementsAs impressionsDay1

    partitionVersions(tableDir) shouldBe Map(
      ("2019-03-13", "2019-03-14") -> List("v1"),
      ("2019-03-14", "2019-03-14") -> List("v1")
    )

    val impressionsDay2 = List(
      AdImpression("user-1", "ad-1", Timestamp.valueOf("2019-03-14 23:59:00"), Date.valueOf("2019-03-15")),
      AdImpression("user-4", "ad-3", Timestamp.valueOf("2019-03-15 00:00:10"), Date.valueOf("2019-03-15"))
    )
    loader.insert(impressionsDay2.toDS().coalesce(2))
    loader.adImpressions().collect() should contain theSameElementsAs impressionsDay1 ++ impressionsDay2

    partitionVersions(tableDir) shouldBe Map(
      ("2019-03-13", "2019-03-14") -> List("v1"),
      ("2019-03-14", "2019-03-14") -> List("v1"),
      ("2019-03-14", "2019-03-15") -> List("v1"),
      ("2019-03-15", "2019-03-15") -> List("v1")
    )

    // Rewrite impressions to change the content for one of the event dates
    val impressionsDay2Updated = impressionsDay2.filter(_.user_id != "user-4")
    loader.insert(impressionsDay2Updated.toDS().coalesce(2))

    // Query to check we see the updated version
    loader.adImpressions().collect() should contain theSameElementsAs impressionsDay1 ++ impressionsDay2Updated

    partitionVersions(tableDir) shouldBe Map(
      ("2019-03-13", "2019-03-14") -> List("v1"),
      ("2019-03-14", "2019-03-14") -> List("v1"),
      ("2019-03-14", "2019-03-15") -> List("v1", "v2"),
      ("2019-03-15", "2019-03-15") -> List("v1")
    )

  }

  def partitionVersions(tableDir: Path): Map[(String, String), List[String]] = {

    def datePartitions(dir: Path): List[String] = {
      println(s"Looking for date partitions in '$dir'")
      val datePartitionPattern = "date=\\d\\d\\d\\d-\\d\\d-\\d\\d"
      dir.toFile.list().toList.filter(_.matches(datePartitionPattern))
    }

    def versions(dir: Path): List[String] = {
      println(s"Looking for versions in '$dir'")
      println(s"dir.toFile.list() = ${dir.toFile.list().toList}")
      dir.toFile.list().toList.filter(_.matches("v\\d+"))
    }

    val impressionDatePartitions: List[String] = datePartitions(tableDir)

    val allPartitions: List[(String, String)] = impressionDatePartitions.flatMap(impressionDate =>
      datePartitions(tableDir.resolve(impressionDate)).map(processedDate => processedDate -> impressionDate))

    allPartitions
      .map {
        case (processedDate, impressionDate) =>
          (processedDate, impressionDate) -> versions(tableDir.resolve(impressionDate).resolve(processedDate))
      }
      .toMap
      .filter { case (_, versions) => versions.nonEmpty }
  }

}