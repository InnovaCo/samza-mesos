package eu.inn.samza.mesos.allocation

trait ResourceMappingStrategy {
  def mapResources[R <: ResourceHolder, T](resourceHolders: List[R], tasks: Set[T], constraints: ResourceConstraints, allocations: Map[HolderId, Set[T]]): Map[R, Set[T]]
}
