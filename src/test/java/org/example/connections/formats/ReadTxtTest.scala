package org.example.connections.formats

import connections.dataLake.ReadTxt
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.io.FileInputStream
import java.util.Properties

class ReadTxtTest extends AnyFunSuite {

  test("Should be return more than one row and 2 columns"){
    val spark = SparkSession.builder().master("local[*]").appName("Test").getOrCreate()

    val props = new Properties()
    val sourcePath = "src/main/resources/config.properties"
    props.load(new FileInputStream(sourcePath))
    val url = props.getProperty("sources")
    val archivo = "Alchemist.txt"

    val readTxt = new ReadTxt()

    val resultDF = readTxt.setDF(spark, url, archivo)

    assert(resultDF.count() > 0)
    assert(resultDF.columns.length > 1)
    assert(archivo.contains(".txt"))
    //assert(resultDF1.isInstanceOf[DataFrame] && resultDF2.isInstanceOf[DataFrame]) // Verifica que ambos sean DataFrames
    //assert(resultDF1 != null && resultDF2 != null) // Verifica que no sean nulos

    spark.stop()
  }

}
