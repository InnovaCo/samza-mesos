package eu.inn.samza.mesos

import eu.inn.samza.mesos.allocation.{MarathonLabelsToAttributesResolver, ResourceConstraints}
import org.scalatest.{FreeSpec, MustMatchers}


class MarathonLabelsToAttributesSpec extends FreeSpec with MustMatchers {

  "MarathonLabelsToAttributes" - {

    "must extract attributes from labels" in {

      val env = Map(
        "MARATHON_APP_LABEL_ABC" → "AAA",
        "MARATHON_APP_LABEL_" → "BBB",
        "MARATHON_APP_LABEL_SOMETHING" → "CCC",
        "MARATHON_APP_PREFIX-XYZ" → "DDD"
      )

      val resolver = new MarathonLabelsToAttributesResolver(env)
      val initial = ResourceConstraints()
        .requireResource("cpus", 0.5)
        .requireResource("mem", 128)
        .requireResource("disk", 256)
        .requireSupport("something", "ololo")

      val resolved = resolver.resolve(initial)

      resolved.resources mustBe Map("cpus" → 0.5, "mem" → 128, "disk" → 256)
      resolved.attributes mustBe Map("abc" → "aaa", "something" → "ccc")
    }
  }
}
