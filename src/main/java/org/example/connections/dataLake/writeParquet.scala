package org.example.connections.dataLake

import org.apache.spark.sql.SparkSession

class writeParquet {

  def getData(spark: SparkSession, sourcePath: String, destinyPath: String): Unit = {
    val df = spark.read.json(sourcePath)
    df.write.mode("overwrite").parquet(destinyPath)
  }

  def getData(spark: SparkSession, sourcePath: String, destinyPath: String, fileInput: String, fileOutput: String): Unit = {
    val df = spark.read.json(sourcePath + fileInput)
    df.write.mode("overwrite").parquet(destinyPath + fileOutput)
  }
}
