package eu.inn.samza.mesos.allocation

import org.apache.samza.config.Config

import scala.util.matching.Regex


class MarathonLabelsToAttributes extends ConstraintsResolverFactory {
  def buildResolver(config: Config): ConstraintsResolver = {
    new MarathonLabelsToAttributesResolver(sys.env)
  }
}

class MarathonLabelsToAttributesResolver(env: Map[String, String]) extends ConstraintsResolver {

  private val LabelPattern = "^MARATHON_APP_LABEL_(.+)".r

  override def resolve(current: ResourceConstraints): ResourceConstraints = {
    extractMapSubset(env, LabelPattern).foldLeft(current) { case (c, (k, v)) ⇒
      c.requireSupport(k.toLowerCase, v.toLowerCase)
    }
  }

  private def extractMapSubset[V](map: Map[String, V], pattern: Regex): Map[String, V] = {
    for {
      kv ← map
      matched ← pattern.findFirstMatchIn(kv._1)
      subkey = matched.group(1)
    } yield {
      subkey → kv._2
    }
  }
}
