package com.gu.tableversions.spark.filesystem

import com.gu.tableversions.spark.{Generators, SparkHiveSuite}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{EitherValues, FreeSpec, Matchers}

class VersionedFileSystemSpec
    extends FreeSpec
    with Matchers
    with EitherValues
    with GeneratorDrivenPropertyChecks
    with Generators
    with SparkHiveSuite {

  "Written config files should be parsed successfully" in forAll(versionedFileSystemConfigGenerator) { conf =>
    VersionedFileSystem.setConfigDirectory(tableUri)
    VersionedFileSystem.writeConfig(conf, spark.sparkContext.hadoopConfiguration)
    val read = VersionedFileSystem.readConfig(tableUri, spark.sparkContext.hadoopConfiguration)
    read.right.value shouldBe conf
  }
}
