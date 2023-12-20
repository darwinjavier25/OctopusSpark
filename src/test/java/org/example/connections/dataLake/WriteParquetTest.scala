package org.example.connections.dataLake

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import java.io.FileInputStream
import java.util.Properties

class WriteParquetTest extends AnyFunSuite {

  test("Should be return success") {
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

    val setDF = new writeParquet

    val parquetFileImput = "test1.json"
    val parquetFileOutput = "json.parquet"
    val df = setDF.getData(spark, sourcePath, desnityPath, parquetFileImput, parquetFileOutput)

    assert(df.isInstanceOf[DataFrame])
    spark.stop()
  }

}
