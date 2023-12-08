package connections.api

trait StandartApiRequest {

  def apiNoAuth(url: String): String = {
    val str = scala.io.Source.fromURL(url).mkString
    str
  }

  def apiAuth(url: String, token: String): String = {
    val link = url + "?api_key=" + token
    val str = scala.io.Source.fromURL(link).mkString
    str
  }
}
