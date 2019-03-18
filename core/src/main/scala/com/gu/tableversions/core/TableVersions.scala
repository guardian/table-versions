package com.gu.tableversions.core

import com.gu.tableversions.core.TableVersions.CommitResult

/**
  * This defines the interface for querying and updating table version information tracked by the system.
  */
trait TableVersions[F[_]] {

  /** Get details about partition versions in a table. */
  def currentVersions(table: TableName): F[TableVersion]

  /** Get a description of which version to write to next for the given partitions of a table. */
  def nextVersions(table: TableName, partitionColumns: List[PartitionColumn]): F[List[PartitionVersion]]

  /**
    * Update partition versions to the given versions.
    * This performs no checking if data has been written to these versions but it will verify that these versions
    * 1) haven't been committed before and 2) these are the next versions to be commited for each of the partitions.
    */
  def commit(newVersion: TableUpdate): F[CommitResult]

}

object TableVersions {

  sealed trait CommitResult
  object SuccessfulCommit extends CommitResult
  final case class InvalidCommit(invalidPartitions: PartitionVersion) extends CommitResult

}
