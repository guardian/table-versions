package com.gu.tableversions.core

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._
import com.gu.tableversions.core.InMemoryTableVersions.TableUpdates
import com.gu.tableversions.core.TableVersions.CommitResult.SuccessfulCommit
import com.gu.tableversions.core.TableVersions.{AddPartitionVersion, PartitionOperation, RemovePartition, TableUpdate}

/**
  * Reference implementation of the table version store. Does not persist state.
  */
class InMemoryTableVersions[F[_]] private (allUpdates: Ref[F, TableUpdates])(implicit F: Sync[F])
    extends TableVersions[F] {

  override def init(table: TableName): F[Unit] =
    allUpdates.update { prev =>
      if (prev.contains(table)) prev else prev + (table -> Nil)
    }

  override def currentVersion(table: TableName): F[TableVersion] =
    // Derive current version of a table by folding over the history of changes
    for {
      allTableUpdates <- allUpdates.get
      tableUpdates <- allTableUpdates
        .get(table)
        .fold(F.raiseError[List[TableUpdate]](new Exception(s"Table '$table' not found")))(F.pure)
      operations = tableUpdates.flatMap(_.partitionUpdates)
    } yield InMemoryTableVersions.applyUpdate(TableVersion.empty)(operations)

  override def nextVersions(table: TableName, partitions: List[Partition]): F[List[PartitionVersion]] = {
    def incrementVersion(partitionVersion: PartitionVersion): PartitionVersion =
      partitionVersion.copy(version = VersionNumber(partitionVersion.version.number + 1))

    for {
      tableVersion <- currentVersion(table)
      nextVersions = partitions.map(
        partition =>
          tableVersion.partitionVersions
            .find(_.partition == partition)
            .map(incrementVersion)
            .getOrElse(PartitionVersion(partition, VersionNumber(1))))
    } yield nextVersions
  }

  override def commit(table: TableName, update: TableVersions.TableUpdate): F[TableVersions.CommitResult] = {
    // Check if the committed partitions match the "next versions" for this
    // If not, error
    // Otherwise add the update to the list

    val applyUpdate: TableUpdates => Either[Exception, TableUpdates] = { currentTableUpdates =>
      // First, just apply the updates to the list in all cases
      // Then do the bit where I actually check that the committed versions match the current 'next' versions

      def updatesMatchCurrentVersions: Boolean = { true } // TODO!

      if (currentTableUpdates.contains(table) && updatesMatchCurrentVersions) {
        val updated = currentTableUpdates + (table -> (currentTableUpdates(table) :+ update))
        Right(updated)
      } else
        Left(new Exception(s"Unknown table '${table.fullyQualifiedName}'"))
    }

    // TODO: response
    InMemoryTableVersions.modifyEither(allUpdates)(applyUpdate).map(_ => SuccessfulCommit)
  }

}

object InMemoryTableVersions {

  // TODO: Put somewhere else, as syntax on `Ref`
  def modifyEither[F[_]: Sync, A, E <: Throwable](ref: Ref[F, A])(f: A => Either[E, A]): F[Unit] =
    ref
      .modify(a =>
        f(a) match {
          case Left(e)     => (a, e.raiseError[F, Unit])
          case Right(newA) => (newA, ().pure[F])
      })
      .flatten

  type TableUpdates = Map[TableName, List[TableUpdate]]

  /**
    * Safe constructor
    */
  def apply[F[_]](implicit F: Sync[F]): F[InMemoryTableVersions[F]] =
    for {
      ref <- Ref[F].of(Map[TableName, List[TableUpdate]]())
      service = new InMemoryTableVersions[F](ref)
    } yield service

  /**
    * Produce current table version based on history of updates.
    */
  private[core] def applyUpdate(initial: TableVersion)(operations: List[PartitionOperation]): TableVersion = {

    def applyOp(agg: Map[Partition, VersionNumber], op: PartitionOperation): Map[Partition, VersionNumber] = op match {
      case AddPartitionVersion(partitionVersion: PartitionVersion) =>
        agg + (partitionVersion.partition -> partitionVersion.version)
      case RemovePartition(partition: Partition) =>
        agg - partition
    }

    val initialVersions: Map[Partition, VersionNumber] =
      initial.partitionVersions.map(pv => pv.partition -> pv.version).toMap

    val latestVersions = operations.foldLeft(initialVersions)(applyOp)

    val latestPartitionVersions = latestVersions.toList.map {
      case (partition, version) => PartitionVersion(partition, version)
    }

    TableVersion(latestPartitionVersions)
  }

}
