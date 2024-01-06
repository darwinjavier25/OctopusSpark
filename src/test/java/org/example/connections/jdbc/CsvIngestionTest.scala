package org.example.connections.jdbc

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import java.io.FileInputStream
import java.util.Properties

class CsvIngestionTest extends AnyFunSuite {

  test("Should be return dataframe and more of one column") {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("DataLakeIngestion")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val props = new Properties()
    val path = "src/main/resources/config.properties"
    props.load(new FileInputStream(path))
    val inputPath = props.getProperty("sources")
    val file = "customerOrders.csv"
    val table = "cust_orders"

    val setDF = new CsvIngestion
    val url = "jdbc:derby:/home/dw/Octopus/SparkOctopus/src/main/resources/demo"
    val df = setDF.tableCreate(spark, inputPath, file, url, table)

    assert(df.isInstanceOf[DataFrame])
    assert(url.contains("jdbc"))
  }

}
