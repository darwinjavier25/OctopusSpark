package sources

import org.apache.spark.sql.SparkSession

class CsvIngestion {

  def tableCreate(spark: SparkSession, path: String, tableName: String): Unit = {
    val df = spark.read.option("header", "true").csv(path)

    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", "")
    connectionProperties.put("password", "")

    val url = "jdbc:derby:/home/dw/Octopus/SparkOctopus/src/main/resources/demo"

    df.write.mode("overwrite")
      .jdbc(url, tableName, connectionProperties)
  }
}
