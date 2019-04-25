package com.gu.tableversions.core

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._
import com.gu.tableversions.core.InMemoryTableVersions._
import com.gu.tableversions.core.TableVersions.CommitResult.SuccessfulCommit
import com.gu.tableversions.core.TableVersions._
import com.gu.tableversions.core.util.RichRef._

/**
  * Reference implementation of the table version store. Does not persist state.
  */
class InMemoryTableVersions[F[_]] private (allUpdates: Ref[F, TableUpdates])(implicit F: Sync[F])
    extends TableVersions[F] {

  override def commit(table: TableName, update: TableVersions.TableUpdate): F[TableVersions.CommitResult] = {
    val applyUpdate: TableUpdates => Either[Exception, TableUpdates] = { currentTableUpdates =>
      if (currentTableUpdates.contains(table)) {
        val updated = currentTableUpdates + (table -> TableState(
          currentVersion = update.header.id,
          updates = currentTableUpdates(table).updates :+ update))
        Right(updated)
      } else
        Left(unknownTableError(table))
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
            Left(unknownCommitId(id))

        case None => Left(unknownTableError(table))
      }
    }

    allUpdates.modifyEither(applyUpdate).void
  }

  override def tableState(table: TableName): F[TableState] =
    for {
      allTableUpdates <- allUpdates.get
      tableState <- allTableUpdates
        .get(table)
        .fold(F.raiseError[TableState](unknownTableError(table)))(F.pure)
    } yield tableState

  override def handleInit(table: TableName)(newTableState: => TableState): F[Unit] =
    allUpdates.update { prev =>
      if (prev.contains(table)) prev
      else {
        prev + (table -> newTableState)
      }
    }
}

object InMemoryTableVersions {

  type TableUpdates = Map[TableName, TableState]

  /**
    * Safe constructor
    */
  def apply[F[_]](implicit F: Sync[F]): F[InMemoryTableVersions[F]] =
    Ref[F].of(Map.empty[TableName, TableState]).map(new InMemoryTableVersions[F](_))

}
