package eu.inn.samza.mesos

import java.util
import java.util.UUID

import eu.inn.samza.mesos.SamzaContainerState.Unclaimed
import eu.inn.samza.mesos.SamzaSchedulerState.ScheduledTask

import scala.collection.JavaConversions._
import org.apache.mesos.Protos._
import org.apache.mesos.{Scheduler, SchedulerDriver}
import org.apache.samza.config.Config
import org.apache.samza.util.Logging
import eu.inn.samza.mesos.MesosConfig._
import eu.inn.samza.mesos.allocation.MesosOfferMapper

import scala.annotation.tailrec


class SamzaScheduler(
                      config: Config,
                      state: SamzaSchedulerState,
                      offerMapper: MesosOfferMapper,
                      registry: ZooRegistry
                    ) extends Scheduler with Logging {

  private val UnhealthyOfferRefuseSeconds = 1
  private val HealthyOfferRefuseSeconds = 5

  private lazy val opsFactory = new MesosOpsFactory(config, state.jobCoordinator.server.getUrl)

  info("Samza scheduler created.")

  def registered(driver: SchedulerDriver, framework: FrameworkID, master: MasterInfo): Unit = {
    info("Samza framework registered")
    info(state)
  }

  def reregistered(driver: SchedulerDriver, master: MasterInfo): Unit =
    info("Samza framework re-registered")

  private def launch(driver: SchedulerDriver, offer: Offer, tasks: Map[ScheduledTask, TaskInfo]) = {
    info(s"Assigning ${tasks.size} tasks to offer ${offer.getId.getValue}.")

    tasks.foreach { case (t, i) ⇒ t.markAsPending(i) }

    info(s"Launching Samza tasks on offer ${offer.getId.getValue} (${offer.getHostname}).")
    val status = acceptOffer(driver, offer.getId, tasks.values.map(opsFactory.launchOp).toSeq)

    if (status != Status.DRIVER_RUNNING) {
      // todo: possible zombie tasks?
      tasks.foreach(_._1.markAsUnclaimed())
    }
  }

  private def reserve(driver: SchedulerDriver, offer: Offer, tasks: util.Set[ScheduledTask]) = {
    info(s"Reserving $tasks using offer ${offer.getId.getValue}.")

    val reservationId = UUID.randomUUID().toString
    val ops = tasks.toList.map(_.containerId).flatMap(c ⇒ opsFactory.reserveAndCreateVolumeOp(c, reservationId, offer))

    acceptOffer(driver, offer.getId, ops)

    tasks foreach { t ⇒
      registry.setReservationFor(t.containerId.samzaId, ReservationHint(reservationId, offer.getSlaveId.getValue))
    }
  }

  private def acceptOffer(driver: SchedulerDriver, offerId: OfferID, ops: Seq[Offer.Operation]) = {
    info(s"Accepting ${offerId.getValue} with operations $ops.")
    val filter = Filters.newBuilder().setRefuseSeconds(UnhealthyOfferRefuseSeconds).build()
    driver.acceptOffers(offerId :: Nil, ops, filter)
  }

  private def declineOffer(driver: SchedulerDriver, offerId: OfferID) = {
    info(s"Declining ${offerId.getValue}.")
    val refuseSeconds = if (state.isHealthy) {
      HealthyOfferRefuseSeconds
    } else {
      UnhealthyOfferRefuseSeconds
    }
    val filter = Filters.newBuilder().setRefuseSeconds(refuseSeconds).build()
    driver.declineOffer(offerId, filter)
  }

  case class MatchedOffer(offer: Offer, allocation: Map[ScheduledTask, TaskInfo])

  @tailrec
  private def matchReservedResources(unclaimed: Set[ScheduledTask], offers: List[Offer], matched: List[MatchedOffer] = Nil): (List[MatchedOffer], Set[ScheduledTask]) = {
    offers match {
      case Nil ⇒
        (matched, unclaimed)
      case x :: xs ⇒
        val offerMatched = (for {
          task ← unclaimed
          hint ← registry.getReservationFor(task.containerId.samzaId)
          resources ← opsFactory.findReservedResources(task.containerId, hint, x)
          taskInfo = opsFactory.buildTaskInfoFor(task.containerId, x.getSlaveId, resources)
        } yield task → taskInfo).toMap

        val stillUnclaimed = unclaimed -- offerMatched.keys

        matchReservedResources(stillUnclaimed, xs, matched :+ MatchedOffer(x, offerMatched))
    }
  }

  private def mapOffers(offers: util.List[Offer], tasks: Set[ScheduledTask]) = {
    offerMapper.mapResources(
      offers.toList,
      tasks,
      state.allocations
    )
  }

  private def allocateReservedResources(driver: SchedulerDriver, offers: util.List[Offer], unclaimed: Set[ScheduledTask]) = {
    info(s"Looking for reserved resources for tasks ${unclaimed.mkString(", ")}.")

    val (matched, unmatched) = matchReservedResources(unclaimed, offers.toList)

    matched match {
      case m if m.forall(_.allocation.isEmpty) ⇒
        info(s"Found no reserved resources.")
        val suggestedMapping = mapOffers(offers, unmatched)

        suggestedMapping foreach { case (offer, tasks) ⇒

          val timedoutTasks = tasks.filter(t ⇒
            t.timeInState > config.schedulerReservationDelay || registry.getReservationFor(t.containerId.samzaId).isEmpty
          )

          if (timedoutTasks.nonEmpty && config.schedulerReservationDelay >= 0) {
            reserve(driver, offer, timedoutTasks)
            timedoutTasks.foreach(_.markAsUnclaimed()) // reset state time to avoid overbooking
          } else {
            declineOffer(driver, offer.getId)
          }
        }

      case m ⇒
        m foreach {
          case MatchedOffer(offer, allocation) ⇒
            if (allocation.isEmpty) {
              info(s"Resource constraints have not been satisfied by offer ${offer.getId.getValue}. Declining.")
              declineOffer(driver, offer.getId)
            } else {
              info(s"Found persistent volume(s) in ${offer.getId.getValue}.")
              launch(driver, offer, allocation)
            }
        }
    }
  }

  private def allocateUnreservedResources(driver: SchedulerDriver, offers: util.List[Offer], unclaimed: Set[ScheduledTask]) = {
    info(s"Trying to allocate tasks ${unclaimed.mkString(", ")} using any available resources.")

    mapOffers(offers, unclaimed) foreach {
      case (offer, tasks) ⇒
        if (tasks.isEmpty) {
          debug(s"Resource constraints have not been satisfied by offer ${offer.getId.getValue}. Declining.")
          declineOffer(driver, offer.getId)
        } else {
          info(s"Resource constraints have been satisfied by offer ${offer.getId.getValue}.")
          launch(
            driver,
            offer,
            tasks.map(t ⇒ t → opsFactory.buildTaskInfoFor(t.containerId, offer.getSlaveId)).toMap)
        }
    }
  }

  def resourceOffers(driver: SchedulerDriver, offers: util.List[Offer]): Unit = {
    if (!state.isHealthy && !state.isShuttingDown) {

      info(s"Offers received: ${offers.toList}")

      val unclaimed = state.allOf(Unclaimed).toSet
      config.schedulerReservationEnabled match {
        case true ⇒
          allocateReservedResources(driver, offers, unclaimed)
        case _ ⇒
          allocateUnreservedResources(driver, offers, unclaimed)
      }

    } else {
      offers.foreach(o ⇒ driver.declineOffer(o.getId))
    }
  }

  def offerRescinded(driver: SchedulerDriver, offer: OfferID): Unit = {}

  override def statusUpdate(driver: SchedulerDriver, status: TaskStatus): Unit = {
    val taskId = status.getTaskId

    info(s"Task [${taskId.getValue}] is in state [${status.getState}]")

    status.getState match {
      case TaskState.TASK_RUNNING ⇒
        state.taskByMesosId(taskId).foreach(_.markAsRunning())

      case TaskState.TASK_FAILED
           | TaskState.TASK_FINISHED
           | TaskState.TASK_KILLED
           | TaskState.TASK_LOST ⇒
        state.taskByMesosId(taskId).foreach(_.markAsUnclaimed())

      case TaskState.TASK_ERROR ⇒
        state.taskByMesosId(taskId).foreach(_.markAsUnclaimed())
        error(s"TASK_ERROR: [${taskId.getValue}] ${status.getMessage}")

      case _ ⇒
    }

    info(state)
  }

  def frameworkMessage(driver: SchedulerDriver, executor: ExecutorID, slave: SlaveID, data: Array[Byte]): Unit = {

  }

  def disconnected(driver: SchedulerDriver): Unit =
    info("Framework has been disconnected")

  def slaveLost(driver: SchedulerDriver, slave: SlaveID): Unit =
    info(s"A slave ${slave.getValue} has been lost")

  def executorLost(driver: SchedulerDriver, executor: ExecutorID, slave: SlaveID, status: Int): Unit =
    info(s"An executor ${executor.getValue} on slave ${slave.getValue} has been lost.")

  def error(driver: SchedulerDriver, error: String): Unit =
    info(s"Error reported: $error")
}