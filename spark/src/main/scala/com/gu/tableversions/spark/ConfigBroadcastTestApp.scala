//package com.gu.tableversions.spark
//
//import org.apache.spark.sql.SparkSession
//import java.util.UUID
//
//object ConfigBroadcastTestApp {
//
//  def main(args: Array[String]): Unit = {
//    val ss = SparkSession
//      .builder()
//      .enableHiveSupport()
//      .getOrCreate()
//
//    import ss.implicits._
//
//    val ds = (1 to 10000).map(i => Foo(i, UUID.randomUUID().toString)).toDS
//    ds.map { _ =>
//        }
//      .count()
//
//    ()
//  }
//
//  case class Foo(x: Int, y: String)
//}
