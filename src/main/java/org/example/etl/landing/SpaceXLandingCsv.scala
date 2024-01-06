package org.example.etl.landing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.example.connections.dataLake.WriteCsv

class SpaceXLandingCsv extends WriteCsv {

  override def ingestDF(spark: SparkSession, sourcePath: String, destinyPath: String, fileImput: String, fileOutput: String): DataFrame = super.ingestDF(spark, sourcePath, destinyPath, fileImput, fileOutput)

}