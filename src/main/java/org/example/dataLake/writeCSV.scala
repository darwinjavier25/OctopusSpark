package dataLake.imputCSV

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

class writeCSV {

  def ingestDF(spark: SparkSession, url: String): Unit = {
    val schema = StructType(Seq(
      StructField("Id", IntegerType, nullable = false),
      StructField("Company", StringType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Friends_number", IntegerType, nullable = true),
      StructField("Scope", DoubleType, nullable = true),
      StructField("Points", DoubleType, nullable = true),
      StructField("Ratio", DoubleType, nullable = true),
      StructField("City", StringType, nullable = true),
      StructField("Industry", StringType, nullable = true),
      StructField("Company_assessment", StringType, nullable = true)
    ))
    val df = spark.read.option("header", "false").schema(schema).csv("/home/dw/Octopus/SparkOctopus/src/main/java/org/example/sources/data/clients.csv")
    df.write.format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .mode("overwrite")
      .save(url)
    println("Success")
  }

}
