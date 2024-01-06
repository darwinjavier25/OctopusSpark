package org.example.connections.dataLake

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class WriteTxt {

  def ingestDfTxt(spark: SparkSession, sourcePath: String, destinyPath: String, fileImput: String, fileOutput: String): DataFrame = {

    val df = spark.read.option("header", "true").csv(sourcePath + fileImput)
    val dfConcat = df.withColumn("all_data", concat_ws(",", df.columns.map(col):_*))
    dfConcat.select("all_data")
      .write
      .mode("overwrite")
      .text(destinyPath + fileOutput)

    print(s"Archivo $fileImput ingestado como $fileOutput")
    df
  }
}
