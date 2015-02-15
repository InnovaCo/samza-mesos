package eu.inn.samza.mesos.allocation

import scala.collection.mutable


class Greedy extends ResourceMappingStrategy {

  def mapResources[R <: ResourceHolder, T](resourceHolders: List[R], tasks: Set[T], constraints: ResourceConstraints, allocations: Map[HolderId, Set[T]]): Map[R, Set[T]] = {
    val unallocated = mutable.Set(tasks.toSeq: _*)
    val holdersMap = mutable.Map.empty[R, Set[T]]

    for (holder ← resourceHolders) {
      holdersMap.put(holder, Set.empty)

      val remaining = TempResourceHolder.fromHolder(holder)

      while (unallocated.nonEmpty && constraints.satisfiedBy(remaining)) {
        holdersMap(holder) += unallocated.head
        unallocated -= unallocated.head
        remaining.decreaseBy(constraints)
      }
    }

    holdersMap.toMap
  }

}
