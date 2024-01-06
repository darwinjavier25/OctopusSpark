package org.example.etl.transformations

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, functions}

class NormalizeData {

  def removeBlancks(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df)((dataframe, column) => dataframe.withColumn(column, functions.trim(col(column))))
    df
  }

  def transformColNames(df: DataFrame): DataFrame = {
    val df2 = df.columns.map(_.toLowerCase().replace(" ", "_"))
    val df3 = df.toDF(df2: _*)
    df3
  }

  def transformDates(df: DataFrame): DataFrame = {
    val df2 = df.withColumn("date_okay", unix_timestamp(col("launch_date"), "dd MMMM yyyy").cast("timestamp"))
    val df3 = df2.withColumn("date_ready", to_date(col("date_okay"), "yyyy-MM-dd"))
    val df4 = df3.withColumn("date_of_launch", to_timestamp(concat(col("date_ready"), lit(" "), col("launch_time")), "yyyy-MM-dd HH:mm"))
    df4
  }

  def transformLower(df: DataFrame): DataFrame = {
    val df1 = df.schema.filter(_.dataType.typeName == "string").map(_.name)
    val df2 = df1.foldLeft(df){
      (acc, column) => acc.withColumn(column, lower(col(column)))
    }
    df2
  }


}
