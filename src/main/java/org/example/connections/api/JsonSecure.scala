package connections.api
import org.apache.spark.sql.{DataFrame, SparkSession}

class JsonSecure extends StandartApiRequest {

  def authResponse(spark: SparkSession, url: String, token: String): DataFrame = {

    import spark.implicits._

    val str = apiAuth(url, token)
    val df = spark.read.json(Seq(str).toDS())
    df
  }
}
