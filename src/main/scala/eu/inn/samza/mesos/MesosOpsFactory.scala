package eu.inn.samza.mesos

import java.net.URL
import java.util
import java.util.UUID

import eu.inn.samza.mesos.MesosConfig.config2Mesos
import org.apache.mesos.Protos._
import org.apache.samza.SamzaException
import org.apache.samza.config.Config
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.job.{CommandBuilder, ShellCommandBuilder}
import org.apache.samza.util.Logging

import scala.collection.JavaConversions._
import scala.concurrent.duration._

class MesosOpsFactory(config: Config, coordinatorUrl: URL) extends Logging {

  private lazy val version = System.currentTimeMillis

  private lazy val jobName = config.getName.get

  private lazy val containerResources =
    buildResource(Resources.Cpu, config.containerMaxCpuCores) ::
    buildResource(Resources.Mem, config.containerMaxMemoryMb) ::
    buildResource(Resources.Disk, config.containerMaxDiskMb) ::
    Nil

  private lazy val throughEnvs =
    config.containerEnvironmentShared
      .flatMap(k ⇒ scala.util.Properties.envOrNone(k).map(v ⇒ k → v)).toMap

  def buildTaskInfoFor(id: SamzaContainerId, slaveId: SlaveID, resources: Seq[Resource]): TaskInfo =
    TaskInfo.newBuilder()
      .setTaskId(mesosTaskId(id))
      .setSlaveId(slaveId)
      .setName(taskName(id))
      .setKillPolicy(
        KillPolicy.newBuilder()
          .setGracePeriod(DurationInfo.newBuilder().setNanoseconds(60.seconds.toNanos))
          .build()
      )
      .setCommand(mesosCommandInfo(id, resources, throughEnvs ++ samzaEnvs(resources)))
      .addAllResources(resources)
      .build()

  def buildTaskInfoFor(id: SamzaContainerId, slaveId: SlaveID): TaskInfo =
    buildTaskInfoFor(id, slaveId, containerResources)

  private def taskName(id: SamzaContainerId) = s"$jobName (container $id)"

  private def taskId(id: SamzaContainerId): String = s"$jobName-container-$id-$version"

  private def samzaEnvs(resources: Seq[Resource]) = {
    config.volumesEnabled match {
      case true ⇒
        resources
          .find(r ⇒ r.hasDisk && r.getDisk.hasVolume)
          .map(_.getDisk.getVolume.getContainerPath)
          //.map(r ⇒ s"/var/tmp/mesos/volumes/roles/${config.schedulerRole}/${r.getDisk.getPersistence.getId}")
          .map("LOGGED_STORE_BASE_DIR" → _)
          .toMap
      case _ ⇒
        Nil
    }
  }

  private def getSamzaCommandBuilder(id: SamzaContainerId): CommandBuilder =
    Class.forName(config.getCommandClass.getOrElse(classOf[ShellCommandBuilder].getName))
      .newInstance.asInstanceOf[CommandBuilder]
      .setConfig(config)
      .setId(id.samzaId)
      .setUrl(coordinatorUrl)

  private def getBuiltMesosCommandInfoURI: CommandInfo.URI =
    CommandInfo.URI.newBuilder()
      .setValue(config.packagePath.getOrElse(throw new SamzaException("Mesos package path is required!")))
      .setExtract(true)
      .build()

  private def getBuiltMesosEnvironment(envMap: util.Map[String, String]): Environment =
    envMap.foldLeft(Environment.newBuilder())({ case (builder, (key, value)) ⇒
      builder.addVariables(
        Environment.Variable.newBuilder()
          .setName(key)
          .setValue(value)
          .build())
    }).build()

  private def mesosTaskId(id: SamzaContainerId): TaskID =
    TaskID.newBuilder()
      .setValue(taskId(id))
      .build()

  private def mesosCommandInfo(id: SamzaContainerId, resources: Seq[Resource], envs: Map[String, String]): CommandInfo = {
    val samzaCommandBuilder = getSamzaCommandBuilder(id)

    CommandInfo.newBuilder()
      .addUris(getBuiltMesosCommandInfoURI)
      .setShell(false)
      .setValue(samzaCommandBuilder.buildCommand())
      .setEnvironment(getBuiltMesosEnvironment(
        samzaCommandBuilder.buildEnvironment() ++ envs
      ))
      .build()
  }

  private def buildResource(resource: ResourceType, value: Double) =
    Resource.newBuilder
      .setName(resource.name)
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder().setValue(value))
      .build()

  def launchOp(taskInfo: TaskInfo): Offer.Operation = {
    val launch = Offer.Operation.Launch.newBuilder()
      .addTaskInfos(taskInfo)
      .build()

    Offer.Operation.newBuilder()
      .setType(Offer.Operation.Type.LAUNCH)
      .setLaunch(launch)
      .build()
  }

  def reservationLabelsFor(containerId: SamzaContainerId, reservationId: String): Labels = {
    val labels = Map(
      "id" → reservationId,
      "job" → jobName,
      "container" → containerId.samzaId.toString
    )

    Labels.newBuilder()
      .addAllLabels(labels.map { case (k, v) ⇒ Label.newBuilder().setKey(k).setValue(v).build() }.toSeq)
      .build()
  }

  def findReservedResources(containerId: SamzaContainerId, reservation: ReservationHint, offer: Offer): Option[Seq[Resource]] = {
    offer.getSlaveId match {
      case slaveId if slaveId.getValue == reservation.slaveId ⇒
        val labels = reservationLabelsFor(containerId, reservation.id)
        val found = offer.getResourcesList
          .filter(r ⇒ r.getRole == config.schedulerRole && r.getReservation.getLabels.equals(labels))
          .toList

        containerResources.flatMap(r ⇒
          found.find(f ⇒ f.getName == r.getName && f.getScalar.getValue >= r.getScalar.getValue)
        ) match {
          case matched if matched.size == containerResources.size ⇒
            Some(matched)
          case _ ⇒
            None // partial or no match
        }
      case _ ⇒
        None
    }
  }

  def reserveAndCreateVolumeOp(containerId: SamzaContainerId, reservationId: String, offer: Offer): Seq[Offer.Operation] =
    reserveOp(containerId, reservationId, containerResources) ::
      createVolumesOp(containerId, reservationId) ::
      Nil

  private def reserveOp(containerId: SamzaContainerId, reservationId: String, resources: Iterable[Resource]): Offer.Operation = {
    import scala.collection.JavaConverters._
    val reservedResources = resources.map { resource ⇒
      Resource.newBuilder(resource)
        .setRole(config.schedulerRole)
        .setReservation(reservationInfo(containerId, reservationId))
        .build()
    }

    val reserve = Offer.Operation.Reserve.newBuilder()
      .addAllResources(reservedResources.asJava)
      .build()

    Offer.Operation.newBuilder()
      .setType(Offer.Operation.Type.RESERVE)
      .setReserve(reserve)
      .build()
  }

  private def diskInfo(containerId: SamzaContainerId, reservationId: String) = {
    val persistence = Resource.DiskInfo.Persistence.newBuilder()
      .setId(s"$jobName-$containerId-$reservationId")

    val volume = Volume.newBuilder()
      .setContainerPath(config.volumesPath)
      .setMode(Volume.Mode.RW)

    Resource.DiskInfo.newBuilder()
      .setPersistence(persistence)
      .setVolume(volume)
  }

  private def reservationInfo(containerId: SamzaContainerId, reservationId: String) = {
    Resource.ReservationInfo.newBuilder()
      .setLabels(reservationLabelsFor(containerId, reservationId))
      .build()
  }

  private def createVolumesOp(containerId: SamzaContainerId, reservationId: String): Offer.Operation = {
    val volume = Resource.newBuilder(containerResources.filter(_.getName == "disk").head)
      .setRole(config.schedulerRole)
      .setDisk(diskInfo(containerId, reservationId))
      .setReservation(reservationInfo(containerId, reservationId))
      .build()

    val create = Offer.Operation.Create.newBuilder()
      .addAllVolumes(volume :: Nil)

    Offer.Operation.newBuilder()
      .setType(Offer.Operation.Type.CREATE)
      .setCreate(create)
      .build()
  }
}
