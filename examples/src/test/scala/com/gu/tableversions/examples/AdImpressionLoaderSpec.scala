package com.gu.tableversions.examples

import com.gu.tableversions.spark.SparkHiveSuite
import org.scalatest.{FlatSpec, Matchers}

class AdImpressionLoaderSpec extends FlatSpec with Matchers with SparkHiveSuite {

  "Writing multiple versions of a snapshot dataset" should "produce distinct versions" ignore {

    // Write ad impressions in turn for event dates 1, 2, and 3,
    // where each date contains some impressions for that date and some from the previous day(s)

    // For each combination of impression date and event date:
    // Query to check we have the right data
    // Check that the underlying storage is in the right place

    // Rewrite impressions to change the content for one of the event dates

    // Query to check we see the updated data

    // Check underlying storage that we have both versions for the updated partition
    // Query Metastore directly to check what partitions the table has?
    // (When implementing rollback and creating views on historical versions we could just test that functionality
    //  instead of querying storage directly)

    // TODO!
  }

}
