package org.example.connections.dataLake

import org.apache.spark.sql.{DataFrame, SparkSession}

class writeParquet {

  def getData(spark: SparkSession, sourcePath: String, destinyPath: String): DataFrame = {
    val df = spark.read.json(sourcePath)
    df.write.mode("overwrite").parquet(destinyPath)
    df
  }

  def getData(spark: SparkSession, sourcePath: String, destinyPath: String, fileInput: String, fileOutput: String): DataFrame = {
    val df = spark.read.json(sourcePath + fileInput)
    df.write.mode("overwrite").parquet(destinyPath + fileOutput)
    df
  }
}
