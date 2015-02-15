package eu.inn.samza.mesos

import eu.inn.samza.mesos.allocation.{Greedy, ResourceConstraints, TempResourceHolder}
import org.scalatest.{FreeSpec, MustMatchers}


class GreedyStrategySpec extends FreeSpec with MustMatchers {

  "Greedy resource mapping strategy" - {

    "must allocate as much resources as possible" in {
      val strategy = new Greedy()

      val constraints = ResourceConstraints()
        .requireResource("cpus", 2.0)
        .requireResource("mem", 512.0)

      val tasks = (1 to 5).toSet

      val offers = List(
        new TempResourceHolder("a", Map("cpus" -> 5.0, "mem" -> 5120.0), Map()),
        new TempResourceHolder("b", Map("cpus" -> 1.0, "mem" -> 1024.0), Map()),
        new TempResourceHolder("c", Map("cpus" -> 5.0, "mem" -> 512.0), Map()))

      val mapped = strategy.mapResources(offers, tasks, constraints, Map.empty)

      mapped.keySet must have size 3

      mapped(offers(0)) must have size 2
      mapped(offers(1)) must have size 0
      mapped(offers(2)) must have size 1
    }

    "must leave unallocated resources as is" in {
      val strategy = new Greedy()

      val constraints = ResourceConstraints()
        .requireResource("cpus", 2.0)
        .requireResource("mem", 512.0)

      val tasks = (1 to 5).toSet

      val offers = List(
        new TempResourceHolder("a", Map("cpus" -> 5.0, "mem" -> 5120.0), Map()),
        new TempResourceHolder("b", Map("cpus" -> 10.0, "mem" -> 5120.0), Map()),
        new TempResourceHolder("c", Map("cpus" -> 5.0, "mem" -> 512.0), Map()))

      val mapped = strategy.mapResources(offers, tasks, constraints, Map.empty)

      mapped.keySet must have size 3

      mapped(offers(0)) must have size 2
      mapped(offers(1)) must have size 3
      mapped(offers(2)) must have size 0
    }

    "must match required attributes" in {
      val strategy = new Greedy()

      val constraints = ResourceConstraints()
        .requireResource("cpus", 2.0)
        .requireResource("mem", 512.0)
        .requireSupport("az", "^(CA|NY)$")
        .requireSupport("size", "BIG")

      val tasks = (1 to 5).toSet

      val offers = List(
        new TempResourceHolder("a", Map("cpus" -> 5.0, "mem" -> 1024.0), Map()),
        new TempResourceHolder("b", Map("cpus" -> 5.0, "mem" -> 1024.0), Map("az" -> "NY", "size" -> "notBIG")),
        new TempResourceHolder("c", Map("cpus" -> 5.0, "mem" -> 1024.0), Map("az" -> "CA", "size" -> "BIG")),
        new TempResourceHolder("d", Map("cpus" -> 5.0, "mem" -> 1024.0), Map("az" -> "NY", "size" -> "BIG")))

      val mapped = strategy.mapResources(offers, tasks, constraints, Map.empty)

      mapped.keySet must have size 4

      mapped(offers(0)) must have size 0
      mapped(offers(1)) must have size 0
      mapped(offers(2)) must have size 2
      mapped(offers(3)) must have size 2
    }
  }
}
