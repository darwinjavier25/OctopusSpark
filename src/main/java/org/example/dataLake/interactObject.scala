package dataLake.imputCSV

import org.apache.spark.sql.SparkSession

object interactObject {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("DataLakeIngestion")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val url = "/home/dw/Octopus/SparkOctopus/src/main/java/org/example/dataLake/csvFiles/ClientsHeaderTrue.csv"
    val writeDF = new writeCSV
    writeDF.ingestDF(spark, url)
  }
}
