package connections.jdbc

import org.apache.spark.sql.{DataFrame, SparkSession}

class JdbcConn {

  def psqlDF(spark: SparkSession, tableName: String, columns: Seq[String]): DataFrame = {

    val selectQuery = s"SELECT ${columns.mkString(",")} FROM $tableName"

    val psqlDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/darwin_psql")
      .option("dbtable", s"($selectQuery) as subquery")
      .option("user", "darwin_psql")
      .option("password", "$Ps21210909sql")
      .load()

    psqlDF

  }

  def psqlDF(spark: SparkSession, tableName: String, columns: Seq[String], where: String): DataFrame = {

    val selectQuery = s"SELECT ${columns.mkString(",")} FROM $tableName where $where"

    val psqlDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/darwin_psql")
      .option("dbtable", s"($selectQuery) as subquery")
      .option("user", "darwin_psql")
      .option("password", "$Ps21210909sql")
      .load()

    psqlDF

  }

  def mysqlDF(spark: SparkSession, tableName: String, columns: Seq[String]): DataFrame = {

    val selectQueryMysql = s"SELECT ${columns.mkString(",")} FROM $tableName"
    //val showMysqlTable = s"SELECT table_name FROM information_schema.tables WHERE table_schema = 'nombre_de_la_base_de_datos"
    val mysqlDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/ResultSet")
      .option("dbtable", s"($selectQueryMysql) as subquery")
      .option("user", "darwin")
      .option("password", "$My21210909sql")
      .load()

    mysqlDF
  }

  def mysqlDF(spark: SparkSession, tableName: String, columns: Seq[String], where: String): DataFrame = {

    val selectQueryMysql = s"SELECT ${columns.mkString(",")} FROM $tableName $where"
    //val showMysqlTable = s"SELECT table_name FROM information_schema.tables WHERE table_schema = 'nombre_de_la_base_de_datos"
    val mysqlDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/ResultSet")
      .option("dbtable", s"($selectQueryMysql) as subquery")
      .option("user", "darwin")
      .option("password", "$My21210909sql")
      .load()

    mysqlDF
  }

  def showTablesMysql(spark: SparkSession): DataFrame = {
    val selectQueryMysql = s"SELECT table_name FROM information_schema.tables WHERE table_schema = 'ResultSet'"
    //val query = s"SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'ResultSet'"
    //val showMysqlTable = s"SELECT table_name FROM information_schema.tables WHERE table_schema = 'nombre_de_la_base_de_datos"
    val mysqlDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/ResultSet")
      .option("dbtable", s"($selectQueryMysql) as subquery")
      .option("user", "darwin")
      .option("password", "$My21210909sql")
      .load()

    mysqlDF
  }

  def showColumnMysql(spark: SparkSession, tableName: String): DataFrame = {
    val selectQueryMysql = s"SELECT table_name AS 'assets', GROUP_CONCAT(column_name ORDER BY ordinal_position) AS '$tableName' FROM information_schema.columns WHERE table_schema = 'ResultSet' and table_name = 'campaigns' GROUP BY table_name"

    //val showMysqlTable = s"SELECT table_name FROM information_schema.tables WHERE table_schema = 'nombre_de_la_base_de_datos"
    val mysqlDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/ResultSet")
      .option("dbtable", s"($selectQueryMysql) as subquery")
      .option("user", "darwin")
      .option("password", "$My21210909sql")
      .load()

    mysqlDF
  }

}
