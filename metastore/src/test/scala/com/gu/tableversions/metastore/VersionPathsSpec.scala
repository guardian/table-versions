package com.gu.tableversions.metastore

import java.net.URI

import com.gu.tableversions.core.VersionNumber
import org.scalatest.{FlatSpec, Matchers}

class VersionPathsSpec extends FlatSpec with Matchers {

  "A path with version 0" should "be the original path" in {
    VersionPaths.pathFor(new URI("s3://foo/bar/"), VersionNumber(0)) shouldBe new URI("s3://foo/bar/")
  }

  it should "return a normalised path if given a path that doesn't end in a '/'" in {
    VersionPaths.pathFor(new URI("s3://foo/bar"), VersionNumber(0)) shouldBe new URI("s3://foo/bar/")
  }

  "A path with a non-0 version number" should "have the version string appended as a folder" in {
    VersionPaths.pathFor(new URI("s3://foo/bar/"), VersionNumber(42)) shouldBe new URI("s3://foo/bar/v42")
  }

}
