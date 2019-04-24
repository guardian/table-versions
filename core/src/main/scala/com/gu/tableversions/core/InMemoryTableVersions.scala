package com.gu.tableversions.core

import java.time.Instant

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._
import com.gu.tableversions.core.InMemoryTableVersions.{TableState, TableUpdates}
import com.gu.tableversions.core.TableVersions.CommitResult.SuccessfulCommit
import com.gu.tableversions.core.TableVersions.TableOperation._
import com.gu.tableversions.core.TableVersions._
import com.gu.tableversions.core.util.RichRef._

/**
  * Reference implementation of the table version store. Does not persist state.
  */
class InMemoryTableVersions[F[_]] private (allUpdates: Ref[F, TableUpdates])(implicit F: Sync[F])
    extends TableVersions[F] {

  override def init(
      table: TableName,
      isSnapshot: Boolean,
      userId: UserId,
      message: UpdateMessage,
      timestamp: Instant): F[Unit] =
    allUpdates.update { prev =>
      if (prev.contains(table)) prev
      else {
        val initialUpdate = TableUpdate(userId, message, timestamp, operations = List(InitTable(table, isSnapshot)))
        val initialTableState = TableState(currentVersion = initialUpdate.header.id, updates = List(initialUpdate))
        prev + (table -> initialTableState)
      }
    }

  override def currentVersion(table: TableName): F[TableVersion] =
    // Derive current version of a table by folding over the history of changes
    // until either the latest or version marked as 'current' is reached.
    for {
      tableState <- tableState(table)
      matchingUpdates = tableState.updates.span(_.header.id != tableState.currentVersion)
      updatesForCurrentVersion = matchingUpdates._1 ++ matchingUpdates._2.take(1)
      operations = updatesForCurrentVersion.flatMap(_.operations)
    } yield
      if (isSnapshotTable(operations))
        InMemoryTableVersions.latestSnapshotTableVersion(operations)
      else
        InMemoryTableVersions.applyPartitionUpdates(PartitionedTableVersion(Map.empty))(operations)

  override def updates(table: TableName): F[List[TableUpdateHeader]] =
    tableState(table).map(_.updates.map(_.header).reverse)

  override def commit(table: TableName, update: TableVersions.TableUpdate): F[TableVersions.CommitResult] = {
    val applyUpdate: TableUpdates => Either[Exception, TableUpdates] = { currentTableUpdates =>
      if (currentTableUpdates.contains(table)) {
        val updated = currentTableUpdates + (table -> TableState(
          currentVersion = update.header.id,
          updates = currentTableUpdates(table).updates :+ update))
        Right(updated)
      } else
        Left(new Exception(s"Unknown table '${table.fullyQualifiedName}'"))
    }

    allUpdates.modifyEither(applyUpdate).as(SuccessfulCommit)
  }

  override def setCurrentVersion(table: TableName, id: CommitId): F[Unit] = {
    val applyUpdate: TableUpdates => Either[Exception, TableUpdates] = { currentTableUpdates =>
      currentTableUpdates.get(table) match {
        case Some(currentTableState) =>
          if (currentTableState.updates.exists(_.header.id == id)) {
            val newTableState = currentTableState.copy(currentVersion = id)
            val updated = currentTableUpdates + (table -> newTableState)
            Right(updated)
          } else
            Left(new Exception(s"Unknown commit ID '$id'"))

        case None => Left(new Exception(s"Unknown table '${table.fullyQualifiedName}'"))
      }
    }

    allUpdates.modifyEither(applyUpdate).void
  }

  private def tableState(table: TableName): F[TableState] =
    for {
      allTableUpdates <- allUpdates.get
      tableState <- allTableUpdates
        .get(table)
        .fold(F.raiseError[TableState](new Exception(s"Unknown table '${table.fullyQualifiedName}'")))(F.pure)
    } yield tableState

  private def isSnapshotTable(operations: List[TableOperation]) = operations match {
    case InitTable(_, isSnapshot) :: _ => isSnapshot
    case _                             => throw new IllegalArgumentException("First operation should be initialising the table")
  }

}

object InMemoryTableVersions {

  case class TableState(currentVersion: CommitId, updates: List[TableUpdate])

  type TableUpdates = Map[TableName, TableState]

  /**
    * Safe constructor
    */
  def apply[F[_]](implicit F: Sync[F]): F[InMemoryTableVersions[F]] =
    Ref[F].of(Map.empty[TableName, TableState]).map(new InMemoryTableVersions[F](_))

  private[core] def latestSnapshotTableVersion(operations: List[TableOperation]): SnapshotTableVersion = {
    val versions = operations.collect {
      case AddTableVersion(version) => version
    }
    SnapshotTableVersion(versions.lastOption.getOrElse(Version.Unversioned))
  }

  /**
    * Produce current table version based on history of updates.
    */
  private[core] def applyPartitionUpdates(initial: PartitionedTableVersion)(
      operations: List[TableOperation]): PartitionedTableVersion = {

    def applyOp(agg: Map[Partition, Version], op: TableOperation): Map[Partition, Version] = op match {
      case AddPartitionVersion(partition: Partition, version: Version) =>
        agg + (partition -> version)
      case RemovePartition(partition: Partition) =>
        agg - partition
      case _: InitTable | _: AddTableVersion => agg
    }

    val latestVersions = operations.foldLeft(initial.partitionVersions)(applyOp)

    PartitionedTableVersion(latestVersions)
  }

}
