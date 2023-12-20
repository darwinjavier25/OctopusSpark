package org.example.connections.jdbc

import org.apache.spark.sql.{DataFrame, SparkSession}

class CsvIngestion {

  def tableCreate(spark: SparkSession, imputPath: String, imputFile: String, jdbcUrl: String, tableName: String): DataFrame = {
    val df = spark.read.option("header", "true").csv(imputPath + imputFile)

    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", "")
    connectionProperties.put("password", "")

    df.write.mode("overwrite")
      .jdbc(jdbcUrl, tableName, connectionProperties)
    df
  }
}
