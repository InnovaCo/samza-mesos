package eu.inn.samza.mesos

import eu.inn.samza.mesos.mapping.{TempResourceHolder, ResourceConstraints, DefaultResourceMappingStrategy}
import org.scalatest.FunSpec
import org.scalatest.ShouldMatchers

class MappingSpec extends FunSpec with ShouldMatchers {

  describe("Default resource mapping strategy") {

    it("should allocate as much resources as possible") {
      val strategy = new DefaultResourceMappingStrategy()

      val constraints = new ResourceConstraints()
        .requireResource("cpus", 2.0)
        .requireResource("mem", 512.0)

      val tasks = (1 to 5).map(_ => new Object).toSet

      val offers = List(
        new TempResourceHolder(Map("cpus" -> 5.0, "mem" -> 5120.0), Map()),
        new TempResourceHolder(Map("cpus" -> 1.0, "mem" -> 1024.0), Map()),
        new TempResourceHolder(Map("cpus" -> 5.0, "mem" -> 512.0), Map()))

      val mapped = strategy.mapResources(offers, tasks, constraints)

      mapped.keySet should have size 3

      mapped(offers(0)) should have size 2
      mapped(offers(1)) should have size 0
      mapped(offers(2)) should have size 1
    }

    it("should leave unallocated resources as is") {
      val strategy = new DefaultResourceMappingStrategy()

      val constraints = new ResourceConstraints()
        .requireResource("cpus", 2.0)
        .requireResource("mem", 512.0)

      val tasks = (1 to 5).map(_ => new Object).toSet

      val offers = List(
        new TempResourceHolder(Map("cpus" -> 5.0, "mem" -> 5120.0), Map()),
        new TempResourceHolder(Map("cpus" -> 10.0, "mem" -> 5120.0), Map()),
        new TempResourceHolder(Map("cpus" -> 5.0, "mem" -> 512.0), Map()))

      val mapped = strategy.mapResources(offers, tasks, constraints)

      mapped.keySet should have size 3

      mapped(offers(0)) should have size 2
      mapped(offers(1)) should have size 3
      mapped(offers(2)) should have size 0
    }

    it("should match required attributes") {
      val strategy = new DefaultResourceMappingStrategy()

      val constraints = new ResourceConstraints()
        .requireResource("cpus", 2.0)
        .requireResource("mem", 512.0)
        .requireSupport("az", "^(CA|NY)$")
        .requireSupport("size", "BIG")

      val tasks = (1 to 5).map(_ => new Object).toSet

      val offers = List(
        new TempResourceHolder(Map("cpus" -> 5.0, "mem" -> 1024.0), Map()),
        new TempResourceHolder(Map("cpus" -> 5.0, "mem" -> 1024.0), Map("az" -> "NY", "size" -> "notBIG")),
        new TempResourceHolder(Map("cpus" -> 5.0, "mem" -> 1024.0), Map("az" -> "CA", "size" -> "BIG")),
        new TempResourceHolder(Map("cpus" -> 5.0, "mem" -> 1024.0), Map("az" -> "NY", "size" -> "BIG")))

      val mapped = strategy.mapResources(offers, tasks, constraints)

      mapped.keySet should have size 4

      mapped(offers(0)) should have size 0
      mapped(offers(1)) should have size 0
      mapped(offers(2)) should have size 2
      mapped(offers(3)) should have size 2
    }
  }
}
