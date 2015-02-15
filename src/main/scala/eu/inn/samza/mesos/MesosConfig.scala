package eu.inn.samza.mesos

import eu.inn.samza.mesos.allocation.{ConstraintsResolver, ConstraintsResolverFactory, RoundRobin}
import org.apache.samza.config.{Config, JobConfig}

import scala.collection.JavaConversions._


object MesosConfig {
  val PACKAGE_PATH                        = "mesos.package.path"
  val MASTER_CONNECT                      = "mesos.master.connect"
  val REGISTRY_CONNECT                    = "mesos.registry.connect"

  val CONTAINER_MAX_MEMORY_MB             = "mesos.container.memory.mb"
  val CONTAINER_MAX_CPU_CORES             = "mesos.container.cpu.cores"
  val CONTAINER_MAX_DISK_MB               = "mesos.container.disk.mb"
  val CONTAINER_ATTRIBUTES                = "mesos.container.attributes"
  val CONTAINER_ENVIRONMENT_SHARED        = "mesos.container.environment.shared"
  val CONTAINER_CONSTRAINTS_RESOLVERS     = "mesos.container.constraints-resolvers"

  val SCHEDULER_USER                      = "mesos.user"
  val SCHEDULER_ROLE                      = "mesos.role"
  val SCHEDULER_RESERVATION_ENABLED       = "mesos.reservation.enabled"
  val SCHEDULER_RESERVATION_DELAY         = "mesos.reservation.delay"
  val SCHEDULER_VOLUMES_ENABLED           = "mesos.volumes.enabled"
  val SCHEDULER_VOLUMES_PATH              = "mesos.volumes.path"
  val SCHEDULER_STRATEGY                  = "mesos.strategy"

  implicit def Config2Mesos(config: Config) = new MesosConfig(config)
}

class MesosConfig(config: Config) extends JobConfig(config) {

  private def jobName = getName.getOrElse("No job.name defined!")

  def containerMaxMemoryMb: Int = getOption(MesosConfig.CONTAINER_MAX_MEMORY_MB).map(_.toInt).getOrElse(1024)

  def containerMaxCpuCores: Double = getOption(MesosConfig.CONTAINER_MAX_CPU_CORES).map(_.toDouble).getOrElse(1)

  def containerMaxDiskMb: Int = getOption(MesosConfig.CONTAINER_MAX_DISK_MB).map(_.toInt).getOrElse(1024)

  def containerAttributes: Map[String, String] =
    subset(MesosConfig.CONTAINER_ATTRIBUTES, true).entrySet().map(e ⇒ e.getKey → e.getValue).toMap

  def containerEnvironmentShared: Set[String] =
    getOption(MesosConfig.CONTAINER_ENVIRONMENT_SHARED).map(_.split(",").toSet).getOrElse(Set.empty).map(_.trim)

  def containerConstraintsResolvers: List[ConstraintsResolver] = get(MesosConfig.CONTAINER_CONSTRAINTS_RESOLVERS, "")
    .split(",", -1).map(_.trim).filter(_.nonEmpty)
    .map(Class.forName(_).newInstance().asInstanceOf[ConstraintsResolverFactory].buildResolver(config))
    .toList

  def packagePath = getOption(MesosConfig.PACKAGE_PATH)

  def masterConnect = getOption(MesosConfig.MASTER_CONNECT)

  def registryConnect = get(MesosConfig.REGISTRY_CONNECT, "not set")

  def schedulerUser = get(MesosConfig.SCHEDULER_USER, "")

  def schedulerRole = get(MesosConfig.SCHEDULER_ROLE, jobName)

  def schedulerReservationEnabled = getBoolean(MesosConfig.SCHEDULER_RESERVATION_ENABLED, false)

  def schedulerReservationDelay = getLong(MesosConfig.SCHEDULER_RESERVATION_DELAY, 3600000L)

  def schedulerStrategy = get(MesosConfig.SCHEDULER_STRATEGY, classOf[RoundRobin].getName)

  def volumesEnabled = getBoolean(MesosConfig.SCHEDULER_VOLUMES_ENABLED, false)

  def volumesPath = get(MesosConfig.SCHEDULER_VOLUMES_PATH, "reserved")
}
