package com.jca

import org.apache.spark.sql.SparkSession

trait spark {
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkScalaExamples")
    .getOrCreate()
}
