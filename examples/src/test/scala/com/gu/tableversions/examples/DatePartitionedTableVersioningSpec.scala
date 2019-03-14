package com.gu.tableversions.examples

import java.sql.{Date, Timestamp}

import org.scalatest.{FlatSpec, Matchers}

class DatePartitionedTableVersioningSpec extends FlatSpec with Matchers {

  case class Pageview(identityId: String, path: String, timestamp: Timestamp, date: Date)

  "Writing multiple versions of a date partitioned datatset" should "produce distinct versions" in {

    // Write pageviews in turn for day 1, 2, and 3

    // For each date:
    // Query to check we have the right data
    // Check that the underlying storage is in the right place

    // Rewrite pageviews to remove one of the identity IDs, affecting only day 2

    // Query to check we see the updated data

    // Check underlying storage that we have both versions for the updated partition
    // Query Metastore directly to check what partitions the table has?
    // (When implementing rollback and creating views on historical versions we could just test that functionality
    //  instead of querying storage directly)
    // TODO!
  }

}
