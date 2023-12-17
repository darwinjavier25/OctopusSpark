package org.example.connections.api

import connections.api.JsonNoSecure
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class JsonNoSecureTest extends AnyFunSuite {

  test("Should be return 2 not null dataframes"){
    val spark = SparkSession.builder().master("local[*]").appName("Test").getOrCreate()

    val apiUrl = "https://data.montgomerycountymd.gov/api/views/v76h-r7br/rows.json?accessType=DOWNLOAD"
    val setDF = new JsonNoSecure
    val (df1, df2) = setDF.fromJson(spark, apiUrl)

    assert(df1.isInstanceOf[DataFrame] && df2.isInstanceOf[DataFrame])
    assert(df1 != null && df2 != null)
  }
}
