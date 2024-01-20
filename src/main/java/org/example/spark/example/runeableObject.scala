package spark.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object runeableObject extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkJoinDataFrames")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  println("Spark here")
  val a1 = new SparkCsv
  val a = a1.listedFakeFriends(spark)
  println("En main")
  a.count()

  val clasDF = new GetDF(spark)
  val df = clasDF.testClaseDf()
  df.show()

  val fakeFriendsDF = new FakeFriends
  println("fakeFriens")
  val df2 = fakeFriendsDF.dataFrame1(spark)

  df2.select("*").filter(col("age").between(18, 23)).show()
  println(df2.getClass.getName)

  val currentMoney = 4000
  val expectNovember = 3000 - (715+50+200)
  val expectDecember = 3000 - (715+240+200)
  val expectTips = 180 + 180
  val depositRoom = 1400
  val spainMoney = 6000
  val probabliIncomes = 3000 + 900

  println(currentMoney + expectNovember + expectDecember + expectTips + depositRoom + spainMoney + probabliIncomes)

  spark.stop()
}
