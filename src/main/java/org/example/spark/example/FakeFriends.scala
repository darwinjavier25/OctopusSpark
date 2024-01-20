package spark.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class FakeFriends {

  def dataFrame1(spark: SparkSession): DataFrame = {

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv("./src/main/java/org/example/connections/formats/data/fakefriendsHeader.csv")

    df.select(col("age"), col("friends")).groupBy("age").avg("friends").sort("age").show()

    val dfAvg = df.groupBy("age").agg(round(avg("friends"), 2).alias("roundFriends"))

    dfAvg
  }
}
