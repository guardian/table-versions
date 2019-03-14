package com.gu.tableversions.examples

import java.sql.{Date, Timestamp}

import org.scalatest.{FlatSpec, Matchers}

class MultiLevelPartitionedTableVersioningSpec extends FlatSpec with Matchers {

  case class AdImpression(
      id: String,
      impressionTimestamp: Timestamp,
      eventTimestamp: Timestamp,
      impressionDate: Date,
      eventDate: Date)

  "Writing multiple versions of a snapshot dataset" should "produce distinct versions" in {

    // Write ad impressions in turn for event dates 1, 2, and 3,
    // where each date contains some impressions for that date and some from the previous day(s)

    // For each combination of impression date and event date:
    // Query to check we have the right data
    // Check that the underlying storage is in the right place

    // Rewrite impressions to change the date for one of the event dates

    // Query to check we see the updated data

    // Check underlying storage that we have both versions for the updated partition
    // Query Metastore directly to check what partitions the table has?
    // (When implementing rollback and creating views on historical versions we could just test that functionality
    //  instead of querying storage directly)

    // TODO!
  }

}
