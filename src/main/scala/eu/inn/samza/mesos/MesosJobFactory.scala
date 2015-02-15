package eu.inn.samza.mesos

import org.apache.samza.job.StreamJobFactory
import org.apache.samza.config.Config

class MesosJobFactory extends StreamJobFactory {
  def getJob(config: Config) = {
    putMDC("jobName", config.getName.getOrElse(throw new SamzaException("can not find the job name")) + "-coordinator")
    putMDC("jobId", config.getJobId.getOrElse("1"))

    new MesosJob(config)
  }
}
