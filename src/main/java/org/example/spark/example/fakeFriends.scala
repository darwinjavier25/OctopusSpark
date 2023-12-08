package spark.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class fakeFriends {

    def dataFrame1(): DataFrame = {

      val spark = SparkSession.builder
        .master("local[*]")
        .appName("SparkJoinDataFrames")
        .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")

      val df = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/dw/Octopus/SparkOctopus/src/main/java/org/example/sources/data/fakefriendsHeader.csv")

      df.select(col("age"), col("friends")).groupBy("age").avg("friends").sort("age").show()

      val dfAvg = df.groupBy("age").agg(round(avg("friends"), 2).alias("roundFriends"))

      spark.stop()

      dfAvg
    }
}
