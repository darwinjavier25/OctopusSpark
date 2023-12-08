package connections

import connections.api.{JsonNoSecure, JsonSecure}
import connections.dataLake.{ReadCvs, ReadTxt}
import connections.jdbc.ReadLocalJDBC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object interactiveDF {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder
      .master("local[*]")
      .appName("SparkJoinDataFrames")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    //val jdbcConn = new jdbcConn

    //println("Tables in MySql")
    //jdbcConn.showTablesMysql(spark).show(false)
    //jdbcConn.showColumnMysql(spark, "campaigns").show(false)


    println("*******Read from JDBC***********")
    val jdbc = new ReadLocalJDBC
    val jdbcDF = jdbc.dataFromJDBC(spark)
    jdbcDF.show()
    jdbcDF.printSchema()


    val dataLakeRead = new ReadCvs
    val fromDataLake = dataLakeRead.setDF(spark)
    println("*******Read csv***********")
    fromDataLake.select("*").show(2, false)

    val wordCount = new ReadTxt
    val df = wordCount.setDF(spark)
    println("*******Read txt***********")
    df.select(sum("count").alias("Total")).show()


    println("*******Read Json Api no secure***********")
    val url = "https://data.montgomerycountymd.gov/api/views/v76h-r7br/rows.json?accessType=DOWNLOAD"
    println(url)
    val apiJson = new JsonNoSecure
    val (expl, expl2) = apiJson.fromJson(spark, url)
    println("*******Read Json data & meta***********")
    expl.show()
    println("*******Explode Json columns***********")
    expl2.select("*").where("category = 'WINE'").show()
    expl2.show()
    //exploreJson.show()


    val token = "tFzaqQPkVK438fX4RphGsEVjZCO7VxgOVEO5yfP3"
    val url2 = "https://api.nasa.gov/planetary/apod"
    val jsonSecure = new JsonSecure
    val dfJson = jsonSecure.authResponse(spark, url2, token)
    println("*******Read Json with Api Key***********")
    dfJson.show(false)

    /*
    val tableName = "attributions"
    val columns = Seq("*")
    val where = "action_id = 69579"
    val attrDF = jdbcConn.psqlDF(spark, tableName, columns, where)
    println("--------postgreSql--------")
    attrDF.show()

    val myTable = "campaigns"
    val myCols = Seq("*")
    val campDF = jdbcConn.mysqlDF(spark, myTable, myCols)
    println("--------mySql--------")
    campDF.show()

    println("Tables in MySql")
    jdbcConn.showTablesMysql(spark).show(false)
    println("---------show tables--------")
    val LisTables = new jdbcConn
    val showColumns = LisTables.showColumnMysql(spark, "campaigns")
    showColumns.show(50, false)

     */

    spark.stop()
  }
}
