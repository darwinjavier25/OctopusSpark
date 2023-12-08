package connections.jdbc

import org.apache.spark.sql.{DataFrame, SparkSession}

class ReadLocalJDBC {

  def dataFromJDBC(spark: SparkSession): DataFrame = {
    val url = "jdbc:derby:/home/dw/Octopus/SparkOctopus/src/main/resources/demo"
    val table = "customers"

    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", "")
    connectionProperties.put("password", "")

    val df = spark.read.jdbc(url, table, connectionProperties)
    df
  }
}
