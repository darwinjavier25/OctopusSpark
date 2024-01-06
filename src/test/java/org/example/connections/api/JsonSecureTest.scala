package org.example.connections.api

import connections.api.JsonSecure
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class JsonSecureTest extends AnyFunSuite {

  test("Should be return 'Connection Success' and dataframe with 8 columns") {
    val spark = SparkSession.builder().master("local[*]").appName("Test").getOrCreate()

    val token = "tFzaqQPkVK438fX4RphGsEVjZCO7VxgOVEO5yfP3"
    val url = "https://api.nasa.gov/planetary/apod"
    val setDF = new JsonSecure
    val conn = setDF.checkConnectionStatus(url + "?api_key=" + token)
    val df = setDF.authResponse(spark, url, token)

    assert(conn.isInstanceOf[String])
    assert(df.isInstanceOf[DataFrame] && df.columns.length == 8)
  }
}
