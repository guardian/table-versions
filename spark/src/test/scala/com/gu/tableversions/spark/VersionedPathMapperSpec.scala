package com.gu.tableversions.spark

import com.gu.tableversions.core.Version
import org.apache.hadoop.fs.Path
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FreeSpec, Matchers}

class VersionedPathMapperSpec extends FreeSpec with Matchers with TableDrivenPropertyChecks {

  val version = Version.generateVersion.unsafeRunSync()

  "VersionedPathModifier" - {
    val modifier = new VersionedPathMapper("s3", version)

    "for paths that look like partition paths" - {
      // @formatter:off
      val partitionMappingTable = Table(
        ("Unversioned partition", "Versioned partition"),

        // Single partition column
        ("versioned://some-bucket/date=2019-01-01", s"s3://some-bucket/date=2019-01-01/${version.label}"),
        ("versioned://some-bucket/date=2019-01-01/", s"s3://some-bucket/date=2019-01-01/${version.label}/"),
        ("versioned://some-bucket/some/path/to/table/date=2019-01-01", s"s3://some-bucket/some/path/to/table/date=2019-01-01/${version.label}"),
        ("versioned://some-bucket/received_date=2019-01-03", s"s3://some-bucket/received_date=2019-01-03/${version.label}"),
        ("versioned://some-bucket/xyz_FOO-baz_42=42", s"s3://some-bucket/xyz_FOO-baz_42=42/${version.label}"),

        // Multiple partition columns
        ("versioned://some-bucket/received_date=2019-01-03/processed_date=2019-01-04", s"s3://some-bucket/received_date=2019-01-03/processed_date=2019-01-04/${version.label}"),
        ("versioned://some-bucket/year=2019/month=jan/day=01", s"s3://some-bucket/year=2019/month=jan/day=01/${version.label}"),
        ("versioned://some-bucket/xyz_FOO-baz_42=42/bar=2019-01-03", s"s3://some-bucket/xyz_FOO-baz_42=42/bar=2019-01-03/${version.label}"),

        // Including file name
        ("versioned://some-bucket/date=2019-01-01/file.parquet", s"s3://some-bucket/date=2019-01-01/${version.label}/file.parquet"),
        ("versioned://some-bucket/date=2019-01-03/some/long/directory/path/file.parquet", s"s3://some-bucket/date=2019-01-03/${version.label}/some/long/directory/path/file.parquet"),

        // Nested directory inside versioned partition
        ("versioned://some-bucket/date=2019-01-01/temp/file.parquet", s"s3://some-bucket/date=2019-01-01/${version.label}/temp/file.parquet"),
        
        // Something that looks like a partition folder in the root part of the table
        ("versioned://some-bucket/some=yes/path/to/table/date=2019-01-01", s"s3://some-bucket/some=yes/path/to/table/date=2019-01-01/${version.label}"),

        // Various valid syntax examples
        ("versioned:/some-bucket/dir/date=2019-01-01", s"s3:/some-bucket/dir/date=2019-01-01/${version.label}"),
        ("versioned:///some-bucket/dir/date=2019-01-01", s"s3:///some-bucket/dir/date=2019-01-01/${version.label}")
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

    "for paths that don't contain partition directories" - {
      "it should just change the scheme when converting a path to the underlying filesystem" in {
        modifier.fromUnderlying(new Path("s3://some-bucket/temp/foo/baz")) shouldBe new Path(
          "versioned://some-bucket/temp/foo/baz")
      }

      "it should just change the scheme when converting a path from the underlying filesystem" in {
        modifier.forUnderlying(new Path("versioned://some-bucket/temp/foo/baz")) shouldBe new Path(
          "s3://some-bucket/temp/foo/baz")
      }
    }

  }
}
