package eu.inn.samza.mesos

import eu.inn.samza.mesos.SamzaSchedulerState.ScheduledTask
import org.apache.mesos.{Protos, SchedulerDriver}
import org.apache.mesos.Protos.{SlaveID, TaskInfo}
import org.apache.samza.config.Config
import org.apache.samza.coordinator.JobCoordinator
import org.apache.samza.job.ApplicationStatus
import org.apache.samza.util.Logging

import scala.collection.JavaConversions._
import SamzaContainerState._


case class SamzaContainerId(samzaId: Int) extends AnyVal {
  override def toString = samzaId.toString
}

sealed trait SamzaContainerState
object SamzaContainerState {
  object Unclaimed extends SamzaContainerState
  object Pending extends SamzaContainerState
  object Running extends SamzaContainerState
}

object SamzaSchedulerState extends Logging {

  private def clock() = System.currentTimeMillis() // todo: extract

  // todo: refactor

  class ScheduledTask(val containerId: SamzaContainerId) {

    private var _state: SamzaContainerState = Unclaimed
    private var _taskInfo: Option[TaskInfo] = None
    private var _stateTimestamp = clock()

    def state = _state
    def taskInfo = _taskInfo

    override def toString = s"Container $containerId (${state.getClass.getSimpleName})"

    def timeInState = clock() - _stateTimestamp

    private def setState(newState: SamzaContainerState) = {
      _state = newState
      _stateTimestamp = clock()
    }

    def markAsUnclaimed() = {
      setState(Unclaimed)
      _taskInfo = None
    }

    def markAsPending(taskInfo: TaskInfo) = {
      setState(Pending)
      _taskInfo = Some(taskInfo)
    }

    def markAsRunning() =
      taskInfo match {
        case Some(_) ⇒
          setState(Running)
        case _ ⇒
          throw new Exception("TaskInfo is missing")
      }
  }

  def apply(config: Config) = {
    new SamzaSchedulerState(JobCoordinator(config), config)
  }
}

class SamzaSchedulerState(val jobCoordinator: JobCoordinator, config: Config) extends Logging {

  @volatile var currentStatus: ApplicationStatus = ApplicationStatus.New

  @volatile var isShuttingDown = false // todo: refactor

  def shutdown(driver: SchedulerDriver) = {
    isShuttingDown = true

    info("Stopping coordinator")
    jobCoordinator.stop

    val prepared = tasks.flatMap(_.taskInfo).map(_.getTaskId)
    info("Killing Mesos tasks: " + prepared.mkString)
    prepared.foreach(driver.killTask)
  }

  def isSafeToStop = allOf(Pending).isEmpty && allOf(Running).isEmpty

  def isHealthy = allOf(Unclaimed).isEmpty && allOf(Pending).isEmpty

  val tasks = jobCoordinator.jobModel.getContainers
    .keySet
    .map(id ⇒ new ScheduledTask(SamzaContainerId(id)))
    .toList

  def allOf(state: SamzaContainerState) =
    tasks.filter(_.state == state)

  def allocations: Map[SlaveID, Set[ScheduledTask]] =
    tasks.flatMap(t ⇒ t.taskInfo.map(ti ⇒ ti.getSlaveId → t)).groupBy(_._1).mapValues(_.map(_._2).toSet)

  def taskByMesosId(id: Protos.TaskID) = tasks.find(_.taskInfo.exists(_.getTaskId == id))

  override def toString = {
    val healthy = if (isHealthy) "HEALTHY" else "NOT HEALTHY"
    s"Job is $healthy. Tasks unclaimed: ${allOf(Unclaimed).size}, pending: ${allOf(Pending).size}, running: ${allOf(Running).size}."
  }
}