package com.gu.tableversions.spark

import java.net.URI

import cats.effect.IO
import com.gu.tableversions.core.{Partition, PartitionSchema}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

// TODO: Turn into syntax on Dataset
object VersionedDataset {

  /**
    * Get the unique partition values that exist within the given dataset, based on given partition columns.
    */
  def partitionValues[T](dataset: Dataset[T], partitionSchema: PartitionSchema)(
      implicit spark: SparkSession): List[Partition] = {
    if (partitionSchema == PartitionSchema.snapshot) {
      List(Partition.snapshotPartition)
    } else {
      // Query dataset for partitions
      // NOTE: this implementation has not been optimised
      val partitionColumnsList = partitionSchema.columns.map(_.name).mkString(", ")
      val partitionsDf = dataset.selectExpr(s"$partitionColumnsList").distinct()
      val partitionRows = partitionsDf.collect().toList

      def rowToPartition(row: Row): Partition = {
        val partitionColumnValues: List[(Partition.PartitionColumn, String)] =
          partitionSchema.columns zip row.toSeq.map(_.toString)

        val columnValues: List[Partition.ColumnValue] = partitionColumnValues.map {
          case (partitionColumn, value) => Partition.ColumnValue(partitionColumn, value)
        }

        Partition(columnValues)
      }

      partitionRows.map(rowToPartition)
    }
  }

  /**
    * Write the given partitioned dataset, storing each partition in the associated path.
    *
    * (A really noddy implementation for for-each'ing over each partition would be fine for now,
    *  we can look into a more clever and performant solution later)
    */
  // TODO: Probably want to take a few more parameters, e.g. format...
  def writeVersionedPartitions[T](dataset: Dataset[T], partitionPaths: Map[Partition, URI]): IO[Unit] = ???

}
