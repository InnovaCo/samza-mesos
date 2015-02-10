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

import eu.inn.samza.mesos.MesosConfig.Config2Mesos
import eu.inn.samza.mesos.mapping.{DefaultResourceMappingStrategy, TaskOfferMapper}
import org.apache.mesos.MesosSchedulerDriver
import org.apache.mesos.Protos.{FrameworkID, FrameworkInfo}
import org.apache.samza.config.Config
import org.apache.samza.job.ApplicationStatus._
import org.apache.samza.job.{ApplicationStatus, StreamJob}
import org.apache.samza.util.Logging

class MesosJob(config: Config) extends StreamJob with Logging {

  val state = new SamzaSchedulerState(config)
  val frameworkInfo = getFrameworkInfo
  val offerMapper = createOfferMapper
  val scheduler = new SamzaScheduler(config, state, offerMapper)
  val driver = new MesosSchedulerDriver(scheduler, frameworkInfo,
    config.getMasterConnect.getOrElse("zk://localhost:2181/mesos"))

  private lazy val version = System.currentTimeMillis()

  sys.addShutdownHook {
    info("Termination signal received. Shutting down.")
    kill
  }

  def getStatus: ApplicationStatus = {
    state.currentStatus
  }

  def getFrameworkInfo: FrameworkInfo = {
    val frameworkName = config.getName.get
    val frameworkId = FrameworkID.newBuilder
      .setValue("%s-%d" format(frameworkName, version))
      .build

    val infoBuilder = FrameworkInfo.newBuilder
      .setName(frameworkName)
      .setId(frameworkId)
      .setUser(config.getUser)
      .setFailoverTimeout(config.getFailoverTimeout)

    config.getRole.foreach(infoBuilder.setRole)

    infoBuilder.build
  }

  def createOfferMapper: TaskOfferMapper = {
    new TaskOfferMapper(new DefaultResourceMappingStrategy)
      .addCpuConstraint(config.getExecutorMaxCpuCores)
      .addMemConstraint(config.getExecutorMaxMemoryMb)
      .addDiskConstraint(config.getExecutorMaxDiskMb)
      .addAttributeConstraint(config.getExecutorAttributes.toSeq: _*)
  }

  def kill: StreamJob = {
    info("Killing current job")
    state.jobCoordinator.stop
    driver.abort()
    state.preparedTasks.values.foreach(t => driver.killTask(t.getTaskId))
    driver.stop()
    state.currentStatus = ApplicationStatus.SuccessfulFinish
    this
  }

  def submit: StreamJob = {
    info("Submitting new job")
    state.jobCoordinator.start
    driver.start()
    state.currentStatus = ApplicationStatus.Running
    this
  }

  def waitForFinish(timeoutMs: Long): ApplicationStatus = {
    val startTimeMs = System.currentTimeMillis()

    while (System.currentTimeMillis() - startTimeMs < timeoutMs) {
      Option(getStatus) match {
        case Some(s) => if (SuccessfulFinish.equals(s) || UnsuccessfulFinish.equals(s)) return s
        case None =>
      }

      Thread.sleep(1000)
    }

    Running
  }

  def waitForStatus(status: ApplicationStatus, timeoutMs: Long): ApplicationStatus = {
    val startTimeMs = System.currentTimeMillis()

    while (System.currentTimeMillis() - startTimeMs < timeoutMs) {
      Option(getStatus) match {
        case Some(s) => if (status.equals(s)) return status
        case None =>
      }

      Thread.sleep(1000)
    }

    Running
  }
}
