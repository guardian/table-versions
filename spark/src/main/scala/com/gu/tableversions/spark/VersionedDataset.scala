package com.gu.tableversions.spark

import java.net.URI

import cats.effect.IO
import com.gu.tableversions.core.{Partition, PartitionSchema}
import org.apache.spark.sql.Dataset

// TODO: Turn into syntax on Dataset
object VersionedDataset {

  /**
    * Get the unique partition values that exist within the given dataset, according to the
    */
  def partitionValues[T](dataset: Dataset[T], partitionSchema: PartitionSchema): List[Partition] = ???

  /**
    * Write the given partitioned dataset, storing each partition in the associated path.
    *
    *       // (A really noddy implementation for for-each'ing over each partition would be fine for now,
    *       //  we can look into a more clever and performant solution later)
    */
  // TODO: Probably want to take a few more parameters, e.g. format...
  def writeVersionedPartitions[T](dataset: Dataset[T], partitionPaths: Map[Partition, URI]): IO[Unit] = ???

}
