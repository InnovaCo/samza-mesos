package eu.inn.samza.mesos.allocation

import org.apache.samza.util.Logging

class RoundRobin extends ResourceMappingStrategy with Logging {

  private var latest = List.empty[String]

  def mapResources[R <: ResourceHolder, T](resourceHolders: List[R], tasks: Set[T], constraints: ResourceConstraints, allocations: Map[HolderId, Set[T]]): Map[R, Set[T]] = {

    debug(s"Mapping $tasks to $resourceHolders")

    val found = resourceHolders
      .filterNot(h ⇒ latest.contains(h.holderId.id))
      .find(constraints.satisfiedBy)
      .orElse(resourceHolders
        .sortWith((l, r) ⇒ latest.indexOf(l.holderId.id) < latest.indexOf(r.holderId.id))
        .find(constraints.satisfiedBy))

    debug(s"Found match: $found")

    found match {
      case Some(h) if tasks.nonEmpty ⇒
        latest = latest.filterNot(h.holderId.id ==) :+ h.holderId.id
        Map(h → Set(tasks.head))
      case _ ⇒
        Map.empty
    }
  }
}
