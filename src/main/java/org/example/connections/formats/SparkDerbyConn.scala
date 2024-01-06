package org.example.connections.formats

import org.apache.spark.sql.{DataFrame, SparkSession}

class SparkDerbyConn {

  def lisTable(spark: SparkSession): DataFrame = {
    val url = "jdbc:derby:/home/dw/Octopus/SparkOctopus/src/main/resources/demo"
    val table = "SYS.SYSTABLES"

    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", "")
    connectionProperties.put("password", "")

    val df = spark.read.jdbc(url, table, connectionProperties)
    val df2 = df.select("*").where("TABLETYPE = 'T'")
    df2
  }

  def displaySchema(spark: org.apache.spark.sql.SparkSession, table: String): Unit = {
    val url = "jdbc:derby:/home/dw/Octopus/SparkOctopus/src/main/resources/demo"

    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", "")
    connectionProperties.put("password", "")

    val df = spark.read.jdbc(url, table, connectionProperties)
    df.printSchema()
  }

}
