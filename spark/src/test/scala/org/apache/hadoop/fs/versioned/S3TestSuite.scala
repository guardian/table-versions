package org.apache.hadoop.fs.versioned

import com.amazonaws.auth.{AWSStaticCredentialsProvider, AnonymousAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.gu.tableversions.spark.SparkHiveSuite
import io.findify.s3mock.S3Mock
import org.scalatest.{BeforeAndAfterAll, Suite}

trait S3TestSuite extends BeforeAndAfterAll {
  self: Suite =>

  val port = 1254

  val api = S3Mock(port = port)

  val endpoint = new EndpointConfiguration(s"http://localhost:$port", "eu-west-1")

  val client = AmazonS3ClientBuilder.standard
    .withPathStyleAccessEnabled(true)
    .withEndpointConfiguration(endpoint)
    .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials))
    .build

  override def beforeAll(): Unit = {
    super.beforeAll()

    api.start

    ()
  }

  override def afterAll(): Unit = {
    super.afterAll()

    api.shutdown
  }
}

trait S3SparkTestSuite {
  self: Suite with SparkHiveSuite with S3TestSuite =>

  import spark.sparkContext.hadoopConfiguration

  hadoopConfiguration.set("fs.s3a.endpoint", s"http://localhost:$port")
//  hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "false")
  hadoopConfiguration.set("fs.s3a.path.style.access", "true")

  hadoopConfiguration.set("fs.s3a.aws.credentials.provider",
                          s"org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")

  hadoopConfiguration.set("fs.s3.impl", s"org.apache.hadoop.fs.s3a.S3AFileSystem")
}
