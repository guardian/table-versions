package com.gu.tableversions.spark

import org.apache.hadoop.fs.Path
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FreeSpec, Matchers}

class VersionedPathMapperSpec extends FreeSpec with Matchers with TableDrivenPropertyChecks {

  "VersionedPathModifier" - {
    val partitionVersions: Map[String, String] = Map(
      "date=2019-01-01" -> "version1",
      "date=2019-01-02" -> "version2",
      "date=2019-01-03" -> "version3"
    )

    val modifier = new VersionedPathMapper("s3", partitionVersions)

    "for paths defined in the mapping" - {
      // @formatter:off
      val partitionMappingTable = Table(
        ("URI under versioned scheme", "URI under S3"),
        ("versioned://some-bucket/date=2019-01-01", "s3://some-bucket/date=2019-01-01/version1"),
        ("versioned://some-bucket/date=2019-01-02", "s3://some-bucket/date=2019-01-02/version2"),
        ("versioned://some-bucket/date=2019-01-03", "s3://some-bucket/date=2019-01-03/version3"),

        ("versioned://some-bucket/date=2019-01-01/file.parquet", "s3://some-bucket/date=2019-01-01/version1/file.parquet"),

        ("versioned://some-bucket/date=2019-01-03/some/long/directory/path/file.parquet", "s3://some-bucket/date=2019-01-03/version3/some/long/directory/path/file.parquet"),

        ("versioned://some-bucket/foo/date=2019-01-02", "s3://some-bucket/foo/date=2019-01-02/version2"),

        ("versioned://some-bucket/date=2019-01-02/x_date=2019-01-02", "s3://some-bucket/date=2019-01-02/version2/x_date=2019-01-02"),
        ("versioned://some-bucket/x_date=2019-01-02/date=2019-01-02", "s3://some-bucket/x_date=2019-01-02/date=2019-01-02/version2")
      )
      // @formatter:on

      "appends versions to input Paths and sets the scheme to the underlying FS" in
        forAll(partitionMappingTable) {
          case (versionedURI, s3URI) =>
            modifier.forUnderlying(new Path(versionedURI)) should equal(new Path(s3URI))
        }

      "removes versions from output Paths and sets the scheme to versioned://" in
        forAll(partitionMappingTable) {
          case (versionedURI, s3URI) =>
            modifier.fromUnderlying(new Path(s3URI)) should equal(new Path(versionedURI))
        }
    }

    "for paths not defined in the mapping" - {
      "it should only change the scheme when converting path to the underlying filesystem" in {
        modifier.fromUnderlying(new Path("s3://some-bucket/date=2019-01-04")) shouldBe new Path(
          "versioned://some-bucket/date=2019-01-04")
      }

      "it should only change the scheme when converting path from the underlying filesystem" in {
        modifier.forUnderlying(new Path("versioned://some-bucket/date=2019-01-04")) shouldBe new Path(
          "s3://some-bucket/date=2019-01-04")
      }
    }

  }
}
