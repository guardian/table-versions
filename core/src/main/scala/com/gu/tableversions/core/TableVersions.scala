package com.gu.tableversions.core

import java.time.Instant
import java.util.UUID

import com.gu.tableversions.core.TableVersions.{CommitResult, TableUpdateHeader, UpdateMessage, UserId}

/**
  * This defines the interface for querying and updating table version information tracked by the system.
  */
trait TableVersions[F[_]] {

  /**
    * Start tracking version information for given table.
    * This must be called before any other operations can be performed on this table.
    */
  def init(table: TableName, isSnapshot: Boolean, userId: UserId, message: UpdateMessage, timestamp: Instant): F[Unit]

  /** Get details about partition versions in a table. */
  def currentVersion(table: TableName): F[TableVersion]

  /** Return the history of table updates, most recent first. */
  def log(table: TableName): F[List[TableUpdateHeader]]

  /**
    * Update partition versions to the given versions.
    */
  def commit(table: TableName, update: TableVersions.TableUpdate): F[CommitResult]

  /**
    * Set the current version of a table to refer to an existing version.
    */
  def setCurrentVersion(table: TableName, id: TableVersions.CommitId): F[Unit]

}

object TableVersions {

  final case class TableUpdateHeader(
      id: CommitId,
      userId: UserId,
      message: UpdateMessage,
      timestamp: Instant
  )

  object TableUpdateHeader {

    def apply(userId: UserId, message: UpdateMessage, timestamp: Instant): TableUpdateHeader =
      TableUpdateHeader(createId(), userId, message, timestamp)

    private def createId(): CommitId = CommitId(UUID.randomUUID().toString)
  }

  final case class CommitId(id: String) extends AnyVal

  final case class UserId(value: String) extends AnyVal

  final case class UpdateMessage(content: String) extends AnyVal

  /** A collection of updates to partitions to be applied and tracked as a single atomic change. */
  final case class TableUpdate(header: TableUpdateHeader, operations: List[TableOperation])

  object TableUpdate {

    def apply(
        userId: UserId,
        message: UpdateMessage,
        timestamp: Instant,
        operations: List[TableOperation]): TableUpdate =
      TableUpdate(TableUpdateHeader(userId, message, timestamp), operations)
  }

  /** Result type for commit operation */
  sealed trait CommitResult

  object CommitResult {
    case object SuccessfulCommit extends CommitResult
    final case class InvalidCommit(invalidPartitions: Map[Partition, ErrorMessage]) extends CommitResult
  }

  case class ErrorMessage(value: String) extends AnyVal

  /** ADT for operations on tables. */
  sealed trait TableOperation

  object TableOperation {
    final case class InitTable(tableName: TableName, isSnapshot: Boolean) extends TableOperation
    final case class AddTableVersion(version: Version) extends TableOperation
    final case class AddPartitionVersion(partition: Partition, version: Version) extends TableOperation
    final case class RemovePartition(partition: Partition) extends TableOperation
  }

}
