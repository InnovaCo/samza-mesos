package eu.inn.samza.mesos.allocation

import org.apache.samza.config.Config

trait ConstraintsResolverFactory {
  def buildResolver(config: Config): ConstraintsResolver
}

trait ConstraintsResolver {
  def resolve(current: ResourceConstraints): ResourceConstraints
}
