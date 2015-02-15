package eu.inn.samza.mesos

import eu.inn.samza.mesos.allocation.{ResourceConstraints, RoundRobin, TempResourceHolder}
import org.scalatest.{FreeSpec, MustMatchers}


class RoundRobinStrategySpec extends FreeSpec with MustMatchers {

  "Round-robin resource mapping strategy" - {

    "must allocate as balanced as possible" in {
      val strategy = new RoundRobin()

      val constraints = ResourceConstraints()
        .requireResource("cpus", 2.0)
        .requireResource("mem", 512.0)

      var tasks = (1 to 5).toSet

      val offers = List(
        new TempResourceHolder("a", Map("cpus" → 10.0, "mem" → 5120.0), Map.empty),
        new TempResourceHolder("b", Map("cpus" → 10.0, "mem" → 512.0), Map.empty),
        new TempResourceHolder("c", Map("cpus" → 10.0, "mem" → 5120.0), Map.empty))

      val map = (1 to 6).flatMap { _ ⇒
        val mapped = strategy.mapResources(offers, tasks, constraints, Map.empty)
        val head = mapped.headOption

        head.foreach { h ⇒
          offers.find(h._1 ==).foreach(_.decreaseBy(constraints))
          tasks -= h._2.head
        }
        head
      }

      map must have size 5
      map.filter(_._2.size == 1) must have size 5

      map.flatMap(_._2) must have size 5

      map.map(_._1.holderId.id) must contain theSameElementsInOrderAs List("a", "b", "c", "a", "c")
    }

    "must allocate as many tasks as possible" in {
      val strategy = new RoundRobin()

      val constraints = ResourceConstraints()
        .requireResource("cpus", 2.0)
        .requireResource("mem", 512.0)

      var tasks = (1 to 10).toSet

      val offers = List(
        new TempResourceHolder("a", Map("cpus" → 4.0, "mem" → 5120.0), Map.empty),
        new TempResourceHolder("b", Map("cpus" → 4.0, "mem" → 256.0), Map.empty),
        new TempResourceHolder("c", Map("cpus" → 4.0, "mem" → 512.0), Map.empty))

      val map = (1 to 10).flatMap { _ ⇒
        val mapped = strategy.mapResources(offers, tasks, constraints, Map.empty)
        val head = mapped.headOption

        head.foreach { h ⇒
          offers.find(h._1 ==).foreach(_.decreaseBy(constraints))
          tasks -= h._2.head
        }
        head
      }

      map must have size 3
      map.map(_._1.holderId.id) must contain theSameElementsInOrderAs List("a", "c", "a")
    }
  }
}
