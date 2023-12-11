package spark.example

import org.apache.spark.sql.{DataFrame, SparkSession}

class GetDF {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkJoinDataFrames")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  def testClaseDf(): DataFrame = {
    val df = spark.read.text("/home/dw/Octopus/SparkOctopus/src/main/java/org/example/sources/data/Alchemist.txt")
    spark.stop()
    df
  }
}
