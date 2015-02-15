package eu.inn.samza

package object mesos {

  class ResourceType(val name: String)
  object Resources {
    object Cpu extends ResourceType("cpus")
    object Mem extends ResourceType("mem")
    object Disk extends ResourceType("disk")
  }
}
