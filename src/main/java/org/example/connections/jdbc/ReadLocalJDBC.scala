package connections.jdbc

import org.apache.spark.sql.{DataFrame, SparkSession}

class ReadLocalJDBC {

  def dataFromJDBC(spark: SparkSession, jdbcUrl: String, tableName: String): DataFrame = {

    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", "")
    connectionProperties.put("password", "")

    val df = spark.read.jdbc(jdbcUrl, tableName, connectionProperties)
    df
  }
}
