package org.example.dataLake

import org.apache.spark.sql.{DataFrame, SparkSession}

class writeParquet {

  def getData(spark: SparkSession, inputUrl: String, outputUrl: String): DataFrame = {
    val df = spark.read.json(inputUrl)
    df.write.mode("overwrite").parquet(outputUrl)
    df
  }
}
