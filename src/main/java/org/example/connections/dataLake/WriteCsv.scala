package org.example.connections.dataLake

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

class WriteCsv {

  def ingestDF(spark: SparkSession, sourcePath: String, destinyPath: String, fileImput: String, fileOutput: String): DataFrame = {

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

    val df = spark.read.option("header", "true").csv(sourcePath + fileImput)
    df.write.format("csv")
          .option("header", "true")
          .option("delimiter", ",")
          .mode("overwrite")
          .save(destinyPath + fileOutput)

    print(s"Archivo $fileImput ingestado como $fileOutput")
    df
    }
}
