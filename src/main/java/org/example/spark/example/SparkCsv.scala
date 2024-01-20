package spark.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class SparkCsv {

  def listedFakeFriends(spark: SparkSession): DataFrame = {

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv("./src/main/java/org/example/connections/formats/data/fakefriendsHeader.csv")

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
    df2
  }
}
