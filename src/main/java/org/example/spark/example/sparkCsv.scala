package spark.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class sparkCsv {

  def listedFakeFriends(): DataFrame = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("SparkJoinDataFrames")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/dw/Octopus/SparkOctopus/src/main/java/org/example/sources/data/fakefriendsHeader.csv")

    //printSchema
    df.printSchema()

    //display columns
    df.select("*").show()

    //Get age ander 35
    df.filter("age < 35").show()

    //group by age
    df.groupBy("age").count().orderBy("count").show()

    //new column, we need first change de type of the column in this case string to int in age
    val df2 = df.withColumn("age", col("age").cast("int"))
    df2.printSchema()
    df2.withColumn("age", expr("age + 10")).show()

    spark.stop()
    df2
  }
}
