package spark.example

import org.apache.spark.sql.{DataFrame, SparkSession}

class GetDF(spark: SparkSession) {

  def testClaseDf(): DataFrame = {
    val df = spark.read.text("./src/main/java/org/example/connections/formats/data/Alchemist.txt")
    df
  }
}
