package eu.inn.samza.mesos.allocation

case class HolderId(id: String) extends AnyVal

trait ResourceHolder {
  def holderId: HolderId

  def resources: Map[String, Double]
  def attributes: Map[String, String]

  def getResource(name: String, default: Double): Double = resources.getOrElse(name, default)
  def getAttribute(name: String, default: String): String = attributes.getOrElse(name, default)
}
