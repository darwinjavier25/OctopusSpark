package connections.dataLake

import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ReadCvs {

  def setDF(spark: SparkSession): DataFrame = {

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/dw/Octopus/SparkOctopus/src/main/java/org/example/sources/data/fakefriendsHeader.csv")

    //Get age ander 35
    val under35 = df.filter("age < 35")

    //group by age
    val groupByAge = df.groupBy("age").count().orderBy("count")

    //new column, we need first change de type of the column in this case string to int in age
    val df2 = df.withColumn("age", col("age").cast("int"))
    val Plus10 = df2.withColumn("age", expr("age + 10"))

    df
  }

}
