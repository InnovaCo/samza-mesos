package eu.inn.samza.mesos

import org.apache.samza.SamzaException
import org.apache.samza.config.Config
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.job.StreamJobFactory
import org.apache.samza.util.Logging


class MesosJobFactory extends StreamJobFactory with Logging {
  def getJob(config: Config) = {
    putMDC("jobName", config.getName.getOrElse(throw new SamzaException("can not find the job name")) + "-coordinator")
    putMDC("jobId", config.getJobId.getOrElse("1"))

    new MesosJob(config)
  }
}