package eu.inn.samza.mesos.allocation

import org.apache.samza.util.Logging


case class ResourceConstraints(
                                resources: Map[String, Double] = Map.empty,
                                attributes: Map[String, String] = Map.empty
                              ) extends Logging {

  def requireResource(resource: String, value: Double): ResourceConstraints = {
    copy(resources = resources + (resource → value))
  }

  def requireSupport(attribute: String, value: String): ResourceConstraints = {
    copy(attributes = attributes + (attribute → value))
  }

  def satisfiedBy(holder: ResourceHolder): Boolean = {
    info(s"Matching offer(${holder.attributes}) and constraints($attributes)")

    attributes.forall(c ⇒ holder.getAttribute(c._1, "").matches(c._2)) &&
      resources.forall(c ⇒ holder.getResource(c._1, 0.0) >= c._2)
  }
}
