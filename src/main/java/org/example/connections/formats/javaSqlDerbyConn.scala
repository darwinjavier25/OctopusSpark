package org.example.connections.formats

import java.sql.{DriverManager, SQLException}

object javaSqlDerbyConn {

  def main(args: Array[String]): Unit = {
    normalDbUsage()
  }
  val dbUrl = "jdbc:derby:/home/dw/Octopus/SparkOctopus/src/main/resources/demo"
  val conn = DriverManager.getConnection(dbUrl)


  @throws[SQLException]
  def normalDbUsage(): Unit = {

    val stmt = conn.createStatement
    // query
    val rs = stmt.executeQuery("SELECT * FROM customers")
    // print out query result
    while (rs.next) println(rs.getString("Customer_id"), rs.getString("Item_id"), rs.getString("Amount_spent"))
  }

}
