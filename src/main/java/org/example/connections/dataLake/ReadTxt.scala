package connections.dataLake

import org.apache.spark.sql.functions.{col, explode, lower, split}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ReadTxt {

  def setDF(spark: SparkSession): DataFrame = {

    val inputDF = spark.read.text("/home/dw/Octopus/SparkOctopus/src/main/java/org/example/sources/data/Alchemist.txt")

    val words = inputDF.select(explode(split(inputDF("value"), "\\W+")).alias("word")).filter(col("word") =!= "")
    val lowCase = words.select(lower(col("word")).alias("words"))
    val groupDF = lowCase.groupBy("words").count()
    val sortedDF = groupDF.orderBy(col("count").desc)
    sortedDF
  }
}
