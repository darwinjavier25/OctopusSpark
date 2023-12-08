package spark.example

import org.apache.spark.sql.functions.col

object scalaMain extends App {

  val a1 = new sparkCsv
  val a = a1.listedFakeFriends()
  println("En main")
  a.count()

  val clasDF = new dataFrame

  val df = clasDF.testClaseDf()

  df.show()

  val fakeFriendsDF = new fakeFriends

  println("fakeFriens")
  val df2 = fakeFriendsDF.dataFrame1()

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
}
