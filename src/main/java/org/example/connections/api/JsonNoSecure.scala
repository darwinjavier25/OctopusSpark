package connections.api

import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.{DataFrame, SparkSession}

class JsonNoSecure extends StandartApiRequest {

  def fromJson(spark: SparkSession, url: String): (DataFrame, DataFrame) = {

    //When you import a implicit scala immediately search for declare implicits and apply this behaviour before of all. In this case is necessary for toDS()
    import spark.implicits._

    val string = apiNoAuth(url)
    val df = spark.read.json(Seq(string).toDS())
    //val explodeDF = df.select(explode(col("meta.view.name").alias("data")))
    //val df2 = df.select("*")
    //df.select(col("meta.view.description"))
    //df.select(col("meta.view.columns.name"), col("meta.view.columns.renderTypeName"))
    //df.select(explode(col("meta.view.columns.name")))
    val explodeDF = df.select(explode($"data") as ("explode_data"))
    val df2 = explodeDF.select(
      $"explode_data"(0).as("row_id"),
      $"explode_data"(1).as("serial_id"),
      $"explode_data"(2).as("number_of_copies"),
      $"explode_data"(3).as("render_type"),
      $"explode_data"(4).as("serial"),
      $"explode_data"(5).as("render_type_2"),
      $"explode_data"(6).as("order_number"),
      $"explode_data"(7).as("reason"),
      $"explode_data"(8).as("year"),
      $"explode_data"(9).as("type"),
      $"explode_data"(10).as("origin"),
      $"explode_data"(11).as("reference"),
      $"explode_data"(12).as("name"),
      $"explode_data"(13).as("category"),
      $"explode_data"(14).as("rating"),
      $"explode_data"(15).as("track_number"),
      $"explode_data"(16).as("version")
    )
    (df, df2)
  }
}
