package sources
import org.apache.spark.sql.SparkSession

object tableCreation {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("SparkJoinDataFrames")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val path = "/home/dw/Octopus/SparkOctopus/src/main/java/org/example/sources/data/customerOrders.csv"
    val table = "customers"
    val insert = new CsvIngestion
    val exect = insert.tableCreate(spark, path, table)
    exect
    spark.stop()
  }
}
