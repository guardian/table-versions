package com.gu.tableversions.glue

import java.net.URI

import cats.effect.IO
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.auth.{
  AWSCredentialsProviderChain,
  EnvironmentVariableCredentialsProvider,
  InstanceProfileCredentialsProvider,
  SystemPropertiesCredentialsProvider
}
import com.amazonaws.regions.Regions
import com.amazonaws.services.glue.model._
import com.amazonaws.services.glue.{AWSGlue, AWSGlueClient}
import com.gu.tableversions.core.Partition.PartitionColumn
import com.gu.tableversions.core._
import com.gu.tableversions.metastore.MetastoreSpec
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import cats.implicits._

import scala.util.Random

class GlueMetastoreSpec extends FlatSpec with Matchers with BeforeAndAfterAll with MetastoreSpec {

  lazy val region = Regions.EU_WEST_1

  lazy val credentials = new AWSCredentialsProviderChain(
    new EnvironmentVariableCredentialsProvider(),
    new SystemPropertiesCredentialsProvider(),
    new ProfileCredentialsProvider("ophan"),
    new ProfileCredentialsProvider(),
    new InstanceProfileCredentialsProvider(false)
  )
  val glue: AWSGlue = AWSGlueClient.builder().withCredentials(credentials).withRegion(region).build()
  val schema = "temp"
  def tableLocation(tableName: String) = new URI(s"s3://ophan-temp-schema/test-data/$tableName")

  val dedupSuffix = Random.alphanumeric.take(8).mkString("")

  val snapshotTable = {
    val tableName = "test_snapshot_" + dedupSuffix
    TableDefinition(TableName(schema, tableName), tableLocation(tableName), PartitionSchema.snapshot)
  }

  val partitionedTable = {
    val tableName = "test_partitioned_" + dedupSuffix

    TableDefinition(TableName(schema, tableName),
                    tableLocation(tableName),
                    PartitionSchema(List(PartitionColumn("date"))))
  }

  "A metastore implementation" should behave like metastoreWithSnapshotSupport(IO { new GlueMetastore(glue) },
                                                                               initTable(snapshotTable),
                                                                               snapshotTable.name,
                                                                               deleteTable(snapshotTable.name))

  it should behave like metastoreWithPartitionsSupport(IO { new GlueMetastore(glue) },
                                                       initTable(partitionedTable),
                                                       partitionedTable.name,
                                                       deleteTable(partitionedTable.name))

  private def initTable(table: TableDefinition): IO[Unit] = {
    def column(name: String, columnType: String) = new Column().withName(name).withType(columnType)
    val storageDescription = new StorageDescriptor()
      .withLocation(table.location.toString)
      .withColumns(column("id", "String"), column("field1", "String"))

    val input = {
      val unpartitionedInput = new TableInput()
        .withName(table.name.name)
        .withDescription("table used in integration tests for table versions")
        .withStorageDescriptor(storageDescription)
      if (table.isSnapshot) unpartitionedInput else unpartitionedInput.withPartitionKeys(column("date", "date"))
    }

    val req = new CreateTableRequest().withDatabaseName(table.name.schema).withTableInput(input)
    IO { glue.createTable(req) }.void
  }

  private def deleteTable(tableName: TableName): IO[Unit] = {
    val deleteRequest = new DeleteTableRequest().withDatabaseName(tableName.schema).withName(tableName.name)
    IO { glue.deleteTable(deleteRequest) }.void
  }

}
