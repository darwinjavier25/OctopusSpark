package org.example.connections.jdbc

import connections.jdbc.ReadLocalJDBC
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class ReadLocalJDBCTest extends AnyFunSuite {

  test("Should be return dataframe") {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("DataLakeIngestion")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val url = "jdbc:derby:/home/dw/Octopus/SparkOctopus/src/main/resources/demo"
    val table = "customers"
    val setDF = new ReadLocalJDBC
    val df = setDF.dataFromJDBC(spark, url, table)
    assert(df.isInstanceOf[DataFrame])
    df.show()
  }

}
