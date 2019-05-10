package com.gu.tableversions.glue

import java.net.URI

import cats.effect.IO
import cats.implicits._
import com.amazonaws.auth._
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.glue.model.{
  Column,
  CreateTableRequest,
  DeleteTableRequest,
  GetPartitionsRequest,
  GetPartitionsResult,
  StorageDescriptor,
  TableInput
}
import com.amazonaws.services.glue.{AWSGlue, AWSGlueClient}
import com.gu.tableversions.core.Partition.PartitionColumn
import com.gu.tableversions.core._
import com.gu.tableversions.metastore.MetastoreSpec
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.util.{Properties, Random}

class GlueMetastoreSpec extends FlatSpec with Matchers with BeforeAndAfterAll with MetastoreSpec {

  def readMandatoryEnvVariable(varName: String): Either[String, String] =
    Properties.envOrNone(varName).toRight(s"$varName environment variable must be set")

  val AWSProfileEnvVarName = "TABLE_VERSIONS_TEST_AWS_PROFILE"
  val AWSRegionEnvVarName = "TABLE_VERSIONS_TEST_AWS_REGION"
  val SchemaEnvVarName = "TABLE_VERSIONS_TEST_GLUE_DATABASE"

  val envVars = for {
    schema <- readMandatoryEnvVariable(SchemaEnvVarName)
    awsRegion <- readMandatoryEnvVariable(AWSRegionEnvVarName)
    awsProfile = Properties.envOrNone(AWSProfileEnvVarName)
  } yield (schema, awsRegion, awsProfile)

  envVars match {
    case Left(error)                                 => cancel(error)
    case Right((schema, awsRegion, maybeAwsProfile)) => runWithVariables(schema, awsRegion, maybeAwsProfile)
  }

  def runWithVariables(schema: String, awsRegion: String, awsProfile: Option[String]): Unit = {
    val providers: List[AWSCredentialsProvider] = {

      List(new EnvironmentVariableCredentialsProvider, new SystemPropertiesCredentialsProvider) ++
        awsProfile.map(new ProfileCredentialsProvider(_)).toList ++
        List(new ProfileCredentialsProvider, new InstanceProfileCredentialsProvider(false))

    }

    lazy val credentials = new AWSCredentialsProviderChain(providers: _*)

    val glue: AWSGlue = AWSGlueClient.builder().withCredentials(credentials).withRegion(awsRegion).build()

    val tableLocation = new URI("/table-versions-test/")

    val dedupSuffix = Random.alphanumeric.take(8).mkString("")

    val snapshotTable = {
      val tableName = "test_snapshot_" + dedupSuffix
      TableDefinition(TableName(schema, tableName), tableLocation, PartitionSchema.snapshot, FileFormat.Parquet)
    }

    val partitionedTable = {
      val tableName = "test_partitioned_" + dedupSuffix

      TableDefinition(TableName(schema, tableName),
                      tableLocation,
                      PartitionSchema(List(PartitionColumn("date"))),
                      FileFormat.Parquet)
    }

    "creating a partition" should "set serde serialization parameters" in {
      import scala.collection.JavaConverters._

      val result = for {
        _ <- initTable(partitionedTable)
        ms = new GlueMetastore[IO](glue)
        dateCol = PartitionColumn("date")
        partition = Partition(dateCol, "2019-01-01")
        version <- Version.generateVersion
        _ <- ms.addPartition(partitionedTable.name, partition, version)
        req = new GetPartitionsRequest()
          .withTableName(partitionedTable.name.name)
          .withDatabaseName(partitionedTable.name.schema)
      } yield (glue.getPartitions(req).getPartitions.asScala, version)

      val (partitions, version) = result.guarantee(deleteTable(partitionedTable.name)).unsafeRunSync()

      partitions should have size 1
      partitions.head.getStorageDescriptor.getLocation should startWith(
        s"/table-versions-test/date=2019-01-01/${version.label}")
      partitions.head.getStorageDescriptor.getInputFormat shouldBe "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
      partitions.head.getStorageDescriptor.getOutputFormat shouldBe "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
      partitions.head.getStorageDescriptor.getSerdeInfo.getSerializationLibrary shouldBe "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    "A metastore implementation" should behave like metastoreWithSnapshotSupport(IO {
      new GlueMetastore(glue)
    }, initTable(snapshotTable), snapshotTable.name, deleteTable(snapshotTable.name))

    it should behave like metastoreWithPartitionsSupport(IO {
      new GlueMetastore(glue)
    }, initTable(partitionedTable), partitionedTable.name, deleteTable(partitionedTable.name))

    def initTable(table: TableDefinition): IO[Unit] = {
      val storageDescription = new StorageDescriptor()
        .withLocation(table.location.toString)
        .withColumns(
          new Column().withName("id").withType("String"),
          new Column().withName("field1").withType("String")
        )

      val input = {
        val unpartitionedInput = new TableInput()
          .withName(table.name.name)
          .withDescription("table used in integration tests for table versions")
          .withStorageDescriptor(storageDescription)

        if (table.isSnapshot)
          unpartitionedInput
        else
          unpartitionedInput.withPartitionKeys(new Column().withName("date").withType("date"))
      }

      val req = new CreateTableRequest().withDatabaseName(table.name.schema).withTableInput(input)
      IO {
        glue.createTable(req)
      }.void
    }

    def deleteTable(tableName: TableName): IO[Unit] = {
      val deleteRequest = new DeleteTableRequest().withDatabaseName(tableName.schema).withName(tableName.name)
      IO {
        glue.deleteTable(deleteRequest)
      }.void
    }
  }

}
