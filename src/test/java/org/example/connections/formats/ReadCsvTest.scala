package org.example.connections.formats

import connections.dataLake.ReadTxt
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.io.FileInputStream
import java.util.Properties

class ReadCsvTest extends AnyFunSuite {

  test("Should be return dataframe and num of columns") {
    val spark = SparkSession.builder().master("local[*]").appName("Test").getOrCreate()

    val props = new Properties()
    val sourcePath = "src/main/resources/config.properties"
    props.load(new FileInputStream(sourcePath))
    val url = props.getProperty("sources")
    val archivo = "clients.csv"

    val readTxt = new ReadTxt()

    val resultDF = readTxt.setDF(spark, url, archivo)

    assert(resultDF.count() > 0)
    assert(resultDF.columns.length > 1)
    assert(archivo.contains(".csv"))

    spark.stop()
  }

}
