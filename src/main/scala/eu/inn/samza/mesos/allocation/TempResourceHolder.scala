package eu.inn.samza.mesos.allocation

object TempResourceHolder {
  def fromHolder(holder: ResourceHolder) = new TempResourceHolder(holder.holderId.id, holder.resources, holder.attributes)
}


case class TempResourceHolder(id: String, resourceList: Map[String, Double], attributeList: Map[String, String]) extends ResourceHolder {

  private var remaining: Map[String, Double] = resourceList

  val holderId = HolderId(id)

  def resources: Map[String, Double] = remaining

  val attributes: Map[String, String] = attributeList

  def decreaseBy(constraints: ResourceConstraints): Unit =
    for((key, value) ← constraints.resources) {
      remaining += key → (remaining.getOrElse(key, 0d) - value)
    }
}
