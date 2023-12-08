package connections.api
import org.apache.spark.sql.{DataFrame, SparkSession}

class JsonSecure extends StandartApiRequest {

  def authResponse(spark: SparkSession, url: String, token: String): DataFrame = {

    import spark.implicits._
    //val token = "tFzaqQPkVK438fX4RphGsEVjZCO7VxgOVEO5yfP3"
    //val url = s"https://api.nasa.gov/planetary/apod?api_key=" + token

    val str = apiAuth(url, token)
    val df = spark.read.json(Seq(str).toDS())
    df
  }
}
