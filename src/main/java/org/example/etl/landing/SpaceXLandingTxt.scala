package org.example.etl.landing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.example.connections.dataLake.WriteTxt

class SpaceXLandingTxt extends WriteTxt {

  override def ingestDfTxt(spark: SparkSession, sourcePath: String, destinyPath: String, fileImput: String, fileOutput: String): DataFrame = super.ingestDfTxt(spark, sourcePath, destinyPath, fileImput, fileOutput)

}
