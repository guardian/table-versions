package com.gu.tableversions.glue

import java.net.URI

import cats.effect.Sync
import cats.implicits._
import com.amazonaws.services.glue.AWSGlue
import com.amazonaws.services.glue.model.{
  CreatePartitionRequest,
  DeletePartitionRequest,
  GetPartitionsRequest,
  GetPartitionsResult,
  GetTableRequest,
  PartitionInput,
  StorageDescriptor,
  TableInput,
  UpdatePartitionRequest,
  UpdateTableRequest,
  Partition => GluePartition,
  Table => GlueTable
}
import com.gu.tableversions.core.Partition.{ColumnValue, PartitionColumn}
import com.gu.tableversions.core.{PartitionedTableVersion, SnapshotTableVersion, TableName, TableVersion, _}
import com.gu.tableversions.metastore.Metastore.TableOperation
import com.gu.tableversions.metastore.Metastore.TableOperation.{
  AddPartition,
  RemovePartition,
  UpdatePartitionVersion,
  UpdateTableVersion
}
import com.gu.tableversions.metastore.{Metastore, VersionPaths}

import scala.collection.JavaConversions._

class GlueMetastore[F[_]](glue: AWSGlue)(implicit F: Sync[F]) extends Metastore[F] {

  override def currentVersion(table: TableName): F[TableVersion] = {

    def getPartitionColumns(glueTable: GlueTable): List[PartitionColumn] =
      Option(glueTable.getPartitionKeys).toList.flatten map { glueColumn =>
        PartitionColumn(glueColumn.getName)
      }

    def snapshotTableVersion(tableLocation: URI): TableVersion =
      SnapshotTableVersion(VersionPaths.parseVersion(tableLocation))

    for {
      glueTable <- getGlueTable(table)
      partitionColumns <- F.delay(getPartitionColumns(glueTable))
      tableLocation <- F.delay(findTableLocation(glueTable))
      version <- if (partitionColumns.isEmpty)
        F.delay(snapshotTableVersion(tableLocation))
      else
        getPartitionedTableVersion(table, tableLocation, partitionColumns)
    } yield (version)
  }

  private def getPartitionedTableVersion(
      table: TableName,
      tableLocation: URI,
      partitionColumns: List[PartitionColumn]): F[TableVersion] = {

    def toPartition(columnValues: List[ColumnValue]): Partition = columnValues match {
      case head :: tail => Partition(head, tail: _*)
      case _            => throw new Exception("empty columnValues list for partition found")
    }

    def toPartitionWithVersion(gluePartition: GluePartition): (Partition, Version) = {

      val partitionColumnAndValue: List[(PartitionColumn, String)] = partitionColumns.zip(gluePartition.getValues)
      val columnValues: List[ColumnValue] = partitionColumnAndValue.map { case (key, value) => ColumnValue(key, value) }
      val partition: Partition = toPartition(columnValues)
      val location = new URI(gluePartition.getStorageDescriptor.getLocation)
      partition -> VersionPaths.parseVersion(location)
    }

    getGluePartitions(table).map { gluePartitions =>
      val partitionVersions: Map[Partition, Version] = gluePartitions.map(toPartitionWithVersion).toMap
      PartitionedTableVersion(partitionVersions)
    }

  }

  override def update(table: TableName, changes: Metastore.TableChanges): F[Unit] =
    changes.operations.traverse_(appliedOp(table))

  private def appliedOp(table: TableName)(operation: TableOperation): F[Unit] =
    operation match {
      case AddPartition(partition, version)           => addPartition(table, partition, version)
      case UpdatePartitionVersion(partition, version) => updatePartitionVersion(table, partition, version)
      case RemovePartition(partition)                 => removePartition(table, partition)
      case UpdateTableVersion(versionNumber)          => updateTableLocation(table, versionNumber)
    }

  def addPartition(table: TableName, partition: Partition, version: Version): F[Unit] = {

    def partitionLocation(tableLocation: URI): String = {
      val unversionedLocation: String = partition.resolvePath(tableLocation).toString
      if (version == Version.Unversioned) unversionedLocation
      else
        unversionedLocation + version.label
    }
    for {
      glueTable <- getGlueTable(table)
      tableLocation <- F.delay(findTableLocation(glueTable))
      location = partitionLocation(tableLocation)
      storageDescriptor <- F.delay(new StorageDescriptor().withLocation(location))
      partitionValues <- F.delay(partition.columnValues.map(_.value).toList)
      input <- F.delay(new PartitionInput().withValues(partitionValues).withStorageDescriptor(storageDescriptor))

      addPartitionRequest = new CreatePartitionRequest()
        .withDatabaseName(table.schema)
        .withTableName(table.name)
        .withPartitionInput(input)

      _ <- F.delay { glue.createPartition(addPartitionRequest) }
    } yield ()
  }

  private def updatePartitionVersion(table: TableName, partition: Partition, version: Version): F[Unit] = {

    def updatePartition(partitionLocation: URI): F[Unit] = {
      val partitionValues = partition.columnValues.map(_.value).toList
      val storageDescriptor = new StorageDescriptor().withLocation(partitionLocation.toString)
      val input = new PartitionInput().withValues(partitionValues).withStorageDescriptor(storageDescriptor)

      val updatePartitionRequest = new UpdatePartitionRequest()
        .withDatabaseName(table.schema)
        .withTableName(table.name)
        .withPartitionInput(input)
        .withPartitionValueList(partitionValues)

      F.delay(glue.updatePartition(updatePartitionRequest)).void
    }

    versionedPartitionLocation(table, partition, version).flatMap(location => updatePartition(location).void)
  }

  private def versionedPartitionLocation(table: TableName, partition: Partition, version: Version): F[URI] =
    for {
      gluetable <- getGlueTable(table)
      tableLocation <- F.delay(findTableLocation(gluetable))
      partitionLocation <- F.delay(partition.resolvePath(tableLocation))
      versionedPartitionLocation <- F.delay(VersionPaths.pathFor(partitionLocation, version))
    } yield versionedPartitionLocation

  def removePartition(table: TableName, partition: Partition): F[Unit] =
    F.delay {
      val partitionvalues = partition.columnValues.map(_.value).toList
      val deletePartitionRequest = new DeletePartitionRequest()
        .withDatabaseName(table.schema)
        .withTableName(table.name)
        .withPartitionValues(partitionvalues)
      glue.deletePartition(deletePartitionRequest)
    }.void

  def updateTableLocation(table: TableName, version: Version): F[Unit] = {
    for {
      glueTable <- getGlueTable(table)
      glueTableLocation <- F.delay(new URI(glueTable.getStorageDescriptor.getLocation))
      basePath <- F.delay(VersionPaths.versionedToBasePath(glueTableLocation))
      versionedPath <- F.delay(VersionPaths.pathFor(basePath, version))
      storageDescriptor <- F.delay(new StorageDescriptor().withLocation(versionedPath.toString))
      tableInput <- F.delay(new TableInput().withName(table.name).withStorageDescriptor(storageDescriptor))
      updateRequest <- F.delay(new UpdateTableRequest().withDatabaseName(table.schema).withTableInput(tableInput))
      res <- F.delay { glue.updateTable(updateRequest) }
    } yield ()
  }

  private[glue] def getGluePartitions(table: TableName): F[List[GluePartition]] = F.delay {
    val req = new GetPartitionsRequest().withDatabaseName(table.schema).withTableName(table.name)
    val getPartitionsResult: GetPartitionsResult = glue.getPartitions(req)
    getPartitionsResult.getPartitions.toList
  }
  private[glue] def getGlueTable(table: TableName): F[GlueTable] = F.delay {
    val getTableRequest = new GetTableRequest().withDatabaseName(table.schema).withName(table.name)
    val getTableResponse = glue.getTable(getTableRequest)
    getTableResponse.getTable
  }
  private[glue] def findTableLocation(glueTable: GlueTable) = {
    val location = glueTable.getStorageDescriptor.getLocation
    new URI(location)
  }
}
