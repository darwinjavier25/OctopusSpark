package connections.api
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

class JsonSecure extends StandartApiRequest {

  def checkConnectionStatus(url: String): String = {
    Try(scala.io.Source.fromURL(url).mkString).map(_ => "Conexión exitosa").getOrElse("Fallo de conexión")
  }
  def authResponse(spark: SparkSession, url: String, token: String): DataFrame = {

    import spark.implicits._

    val str = apiAuth(url, token)
    val df = spark.read.json(Seq(str).toDS())
    df
  }
}
