package org.example.connections.formats

import org.apache.spark.sql.{DataFrame, SparkSession}

class ReadJson {

  def setDfJsonSource(spark: SparkSession, sourcePath: String, file: String): DataFrame = {
    val df = spark.read.json(sourcePath + file)
    df
  }
}
