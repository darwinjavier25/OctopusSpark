package org.example.etl.landing

import connections.api.JsonNoSecure
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.FileInputStream
import java.util.Properties

class SpaceXLandingApi extends JsonNoSecure {

  def landJson(spark: SparkSession, endPoint: String, fileNameDest: String): DataFrame = {
    val url = "https://api.spacexdata.com/v3/" + endPoint
    val setDF = new JsonNoSecure
    val df = setDF.fromJson(spark, url)

    val props = new Properties()
    val path = "src/main/resources/config.properties"
    props.load(new FileInputStream(path))
    val urlDest = props.getProperty("destiny")
    df.write.mode("overwrite").json(urlDest + "/" + fileNameDest)
    df
  }

}
