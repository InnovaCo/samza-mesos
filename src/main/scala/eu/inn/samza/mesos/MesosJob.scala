package eu.inn.samza.mesos

import org.apache.mesos.MesosSchedulerDriver
import org.apache.mesos.Protos.{FrameworkID, FrameworkInfo}
import org.apache.samza.config.Config
import org.apache.samza.job.ApplicationStatus._
import org.apache.samza.job.{ApplicationStatus, StreamJob}
import org.apache.samza.util.Logging
import eu.inn.samza.mesos.MesosConfig.config2Mesos
import eu.inn.samza.mesos.allocation.{ResourceConstraints, ResourceMappingStrategy, MesosOfferMapper}


class MesosJob(config: Config) extends StreamJob with Logging {

  val state = SamzaSchedulerState(config)

  val frameworkInfo = {
    val frameworkName = config.getName.get

    val frameworkId = FrameworkID.newBuilder
      .setValue(s"$frameworkName-$version")
      .build

    FrameworkInfo.newBuilder
      .setName(frameworkName)
      .setId(frameworkId)
      .setUser(config.schedulerUser)
      .setFailoverTimeout(0)
      .setRole(config.schedulerRole)
      .build
  }

  val offerMapper = {
    val strategy = Class.forName(config.schedulerStrategy).newInstance().asInstanceOf[ResourceMappingStrategy]

    val initialConstraints = ResourceConstraints(
      Map(
        "cpus" → config.containerMaxCpuCores,
        "mem" → config.containerMaxMemoryMb,
        "disk" → config.containerMaxDiskMb
      ),
      config.containerAttributes
    )

    val fullConstraints = config.containerConstraintsResolvers.foldLeft(initialConstraints)((c, r) ⇒ r.resolve(c))

    new MesosOfferMapper(fullConstraints, strategy)
  }

  info(s"Create SamzaScheduler with $config")

  info(s"Using allocation strategy ${config.schedulerStrategy}")

  val registry = ZooRegistry(config)

  val scheduler = new SamzaScheduler(config, state, offerMapper, registry)

  val driver = new MesosSchedulerDriver(scheduler, frameworkInfo, config.masterConnect.getOrElse("zk://localhost:2181/mesos"))

  private lazy val version = System.currentTimeMillis()

  sys.addShutdownHook {
    info("Termination signal received. Shutting down.")
    kill
    waitForFinish(30000) match {
      case Running ⇒ warn("Job is still RUNNING")
      case SuccessfulFinish ⇒ info("Job is SUCCESSFULLY FINISHED")
      case UnsuccessfulFinish ⇒ warn("Job is UNSUCCESSFULLY FINISHED")
      case status ⇒ warn(s"Job is in status: $status")
    }
  }

  def getStatus: ApplicationStatus = state.currentStatus

  def kill: StreamJob = {
    info("Killing current job")

    state.shutdown(driver)

    while (!state.isSafeToStop) { // todo: refactor
      info("Waiting for all tasks to gracefully stop")
      Thread.sleep(1000)
    }

    info("Aborting Mesos driver")
    driver.abort()

    info("Stopping Mesos driver")
    driver.stop()
    state.currentStatus = ApplicationStatus.SuccessfulFinish

    info("Done killing current job")
    this
  }

  def submit: StreamJob = {
    info("Submitting new job")

    state.jobCoordinator.start
    driver.start()
    state.currentStatus = ApplicationStatus.Running
    this
  }

  def waitForFinish(timeoutMs: Long): ApplicationStatus =
    checkStatus(System.currentTimeMillis, timeoutMs)(s ⇒ s == SuccessfulFinish || s == UnsuccessfulFinish)

  def waitForStatus(status: ApplicationStatus, timeoutMs: Long): ApplicationStatus =
    checkStatus(System.currentTimeMillis(), timeoutMs)(_ == status)

  @annotation.tailrec
  private def checkStatus(startTimeMs: Long, timeoutMs: Long)(predicate: ApplicationStatus ⇒ Boolean): ApplicationStatus = {
    info(s"Starting waiting for $timeoutMs ms. Current status  $getStatus")
    if (System.currentTimeMillis - startTimeMs < timeoutMs) {
      Option(getStatus) match {
        case Some(s) if predicate(s) ⇒ s
        case other ⇒
          info(s"Waiting for $timeoutMs ms. Current status $other")
          Thread.sleep(1000)
          checkStatus(startTimeMs, timeoutMs)(predicate)
      }
    } else Running
  }
}