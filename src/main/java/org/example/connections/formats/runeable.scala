package org.example.connections.formats

import org.apache.spark.sql.SparkSession

object runeable {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("SparkJoinDataFrames")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    /*
    val imputPath = "/home/dw/Octopus/SparkOctopus/src/main/java/org/example/sources/data/"
    val imputFile = "customerOrders.csv"
    val table = "customers"
    val insert = new CsvIngestion
    insert.tableCreate(spark, imputPath, imputFile, table)
     */

    val setListTable = new SparkDerbyConn
    val showList = setListTable.lisTable(spark)
    showList.show()
    setListTable.displaySchema(spark, "USERS")
    setListTable.displaySchema(spark, "FRIENDS")
    setListTable.displaySchema(spark, "TEMPERATURE")
    setListTable.displaySchema(spark, "CUSTOMERS")
    spark.stop()
  }
}
