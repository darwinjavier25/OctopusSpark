package org.example.connections.dataLake

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import java.io.FileInputStream
import java.util.Properties

class WriteCsvTest extends AnyFunSuite {

  test(s"Should be return ok"){
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("DataLakeIngestion")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val props = new Properties()
    val path = "src/main/resources/config.properties"
    props.load(new FileInputStream(path))
    val sourcePath = props.getProperty("sources")
    val desnityPath = props.getProperty("destiny")

    val csvFileInput = "clients.csv"
    val csvFileOutput = "clientsSchema.csv"
    val writeDF = new WriteCsv
    val df = writeDF.ingestDF(spark, sourcePath, desnityPath, csvFileInput, csvFileOutput)

    assert(df.isInstanceOf[DataFrame])
    assert(csvFileOutput.contains(".csv"))

    spark.stop()
  }
}
