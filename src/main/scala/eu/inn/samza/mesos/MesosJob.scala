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

import java.util.Calendar

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

  def getStatus: ApplicationStatus = {
    state.currentStatus
  }

  def getFrameworkInfo: FrameworkInfo = {
    val frameworkName = config.getName.get
    val frameworkId = FrameworkID.newBuilder
      .setValue("%s-%d" format(frameworkName, Calendar.getInstance().getTimeInMillis))
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
      .addAttributeConstraint(config.getExecutorAttributes.toSeq: _*)
  }

  def kill: StreamJob = {
    state.jobCoordinator.stop
    driver.stop
    this
  }

  def submit: StreamJob = {
    state.jobCoordinator.start
    driver.run
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
