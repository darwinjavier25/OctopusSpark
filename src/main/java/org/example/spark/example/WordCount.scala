package spark.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class WordCount {

  def conteo(spark: SparkSession): DataFrame = {

    val imputDF = spark.read.text("./src/main/java/org/example/connections/formats/data/Alchemist.txt")

    imputDF.printSchema()
    println(imputDF.getClass.getName)
    //imputDF.show()

    val words = imputDF.select(explode(split(imputDF("value"), "\\W+")).alias("word")).filter(col("word") =!= "")
    val lowerWords = words.select(lower(col("word")).alias("word"))
    val wordCounts = lowerWords.groupBy("word").count()
    val sortedCount = wordCounts.orderBy(col("count").asc)
    sortedCount.show()
    //sortedCount.filter("count > 200").show()
    sortedCount.select(sum("count")).show(50, false)
    sortedCount.filter("word = 'her'").show()
    sortedCount.filter("count == 15").show()
    sortedCount
  }
}
