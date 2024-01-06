package org.example.connections.dataLake

import org.apache.spark.sql.SparkSession

import java.io.FileInputStream
import java.util.Properties

object runeableDF {

  def main(args: Array[String]): Unit = {
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
    writeDF.ingestDF(spark, sourcePath, desnityPath, csvFileInput, csvFileOutput)


    val setDF = new writeParquet
    /*
    val urlImput = "/home/dw/Octopus/SparkOctopus/src/main/java/org/example/sources/data/test1.json"
    val urlOutput = "/home/dw/Octopus/SparkOctopus/src/main/java/org/example/dataLake/parquetFiles/json.parquet"
    setDF.getData(spark, urlImput, urlOutput)

     */

    val parquetFileImput = "test1.json"
    val parquetFileOutput = "json.parquet"
    setDF.getData(spark, sourcePath, desnityPath, parquetFileImput, parquetFileOutput)

    spark.stop()
  }
}
