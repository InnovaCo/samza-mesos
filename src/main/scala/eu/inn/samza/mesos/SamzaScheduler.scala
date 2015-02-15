/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package eu.inn.samza.mesos

import java.util

import eu.inn.samza.mesos.mapping.TaskOfferMapper
import org.apache.mesos.Protos._
import org.apache.mesos.{Scheduler, SchedulerDriver}
import org.apache.samza.config.Config
import org.apache.samza.util.Logging

import scala.collection.JavaConversions._

class SamzaScheduler(config: Config,
                     state: SamzaSchedulerState,
                     offerMapper: TaskOfferMapper) extends Scheduler with Logging {

  info("Samza scheduler created.")

  def registered(driver: SchedulerDriver, framework: FrameworkID, master: MasterInfo) {
    info("Samza framework registered")
  }

  def reregistered(driver: SchedulerDriver, master: MasterInfo): Unit = {
    info("Samza framework re-registered")
  }

  def launch(driver: SchedulerDriver, offer: Offer, tasks: util.Set[MesosTask]): Unit = {
    info("Assigning %d tasks to offer %s." format(tasks.size(), offer.getId.getValue))
    val preparedTasks = tasks map (_.getBuiltMesosTaskInfo(offer.getSlaveId))

    info("Launching Samza tasks on offer %s." format offer.getId.getValue)
    val status = driver.launchTasks(offer.getId :: Nil, preparedTasks)

    debug("Result of tasks launch is %s".format(status))

    if (status == Status.DRIVER_RUNNING) {
      state.preparedTasks ++= preparedTasks.map(p => (p.getTaskId.getValue, p)).toMap
      state.pendingTasks ++= preparedTasks.map(_.getTaskId.getValue)
      state.unclaimedTasks --= preparedTasks.map(_.getTaskId.getValue)
    }
    // todo: else what?
  }

  private def allocateResources(driver: SchedulerDriver, offers: util.List[Offer]): Unit = {
    info("Trying to allocate tasks using existing offers.")

    offerMapper.mapResources(offers.toList, state.filterTasks(state.unclaimedTasks.toSeq))
      .foreach(kv => {
      if (kv._2.isEmpty) {
        debug("Resource constraints have not been satisfied by offer %s. Declining." format kv._1.getId.getValue)
        driver.declineOffer(kv._1.getId)
      } else {
        info("Resource constraints have been satisfied by offer %s." format kv._1.getId.getValue)
        launch(driver, kv._1, kv._2)
      }
    })
  }

  def resourceOffers(driver: SchedulerDriver, offers: util.List[Offer]) {
    if (state.unclaimedTasks.nonEmpty) {
      allocateResources(driver, offers)
    } else {
      offers.foreach(o => driver.declineOffer(o.getId))
    }
  }

  def offerRescinded(driver: SchedulerDriver, offer: OfferID): Unit = {}

  override def statusUpdate(driver: SchedulerDriver, status: TaskStatus) {
    val taskId = status.getTaskId.getValue

    info("Task [%s] is in state [%s]" format(taskId, status.getState))

    status.getState match {
      case TaskState.TASK_RUNNING =>
        state.pendingTasks -= taskId
        state.runningTasks += taskId
      case TaskState.TASK_FAILED
           | TaskState.TASK_FINISHED
           | TaskState.TASK_KILLED
           | TaskState.TASK_LOST =>
        state.unclaimedTasks += taskId
        state.pendingTasks -= taskId
        state.runningTasks -= taskId
      case _ =>
    }

    state.isHealthy = state.runningTasks.size == state.initialTaskCount
    state.dump()
  }

  def frameworkMessage(driver: SchedulerDriver,
                       executor: ExecutorID,
                       slave: SlaveID,
                       data: Array[Byte]): Unit = {
  }

  def disconnected(driver: SchedulerDriver): Unit = {
    info("Framework has been disconnected")
  }

  def slaveLost(driver: SchedulerDriver, slave: SlaveID): Unit = {
    info("A slave %s has been lost" format slave.getValue)
  }

  def executorLost(driver: SchedulerDriver,
                   executor: ExecutorID,
                   slave: SlaveID,
                   status: Int): Unit = {
    info("An executor %s on slave %s has been lost." format(executor.getValue, slave.getValue))
  }

  def error(driver: SchedulerDriver, error: String) {
    info("Error reported: %s" format error)
  }
}
