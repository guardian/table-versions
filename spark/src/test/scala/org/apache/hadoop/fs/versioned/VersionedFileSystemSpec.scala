package org.apache.hadoop.fs.versioned

import java.net.URI

import com.gu.tableversions.core.Partition
import com.gu.tableversions.core.Partition.{ColumnValue, PartitionColumn}
import org.apache.hadoop.fs.Path
import org.scalatest.{FlatSpec, Matchers}

class VersionedFileSystemSpec extends FlatSpec with Matchers {

  "partitionMappings" should "produce configuration entries for each partition" in {

    val m =
      List(Partition(ColumnValue(PartitionColumn("date"), "2019-01-01"), ColumnValue(PartitionColumn("hour"), "01")))

    VersionedFileSystem.partitionMappings(m) shouldBe Map(
      "versioned__date=2019-01-01/hour=01" -> "date=2019-01-01/hour=01")

  }

  "appendVersion" should "add a version to a given path" ignore {

    val fs = new VersionedFileSystem
    fs.initialize(new URI(s"file:///tmp/test"), ???)

    val path = new Path(new URI(s"file:///tmp/test"))

//    VersionedFileSystem.partitionMappings(m) shouldBe Map(
//      "versioned__date=2019-01-01/hour=01" -> "date=2019-01-01/hour=01")

  }
}
