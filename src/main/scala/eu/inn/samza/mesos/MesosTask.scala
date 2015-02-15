package eu.inn.samza.mesos

import java.util

import org.apache.mesos.Protos._
import org.apache.samza.config.Config
import eu.inn.samza.mesos.MesosConfig.Config2Mesos
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.job.{CommandBuilder, ShellCommandBuilder}

import scala.collection.JavaConversions._

class MesosTask(config: Config,
                state: SamzaSchedulerState,
                val samzaTaskId: Int) {

  private lazy val version = System.currentTimeMillis()

  def getMesosTaskName: String = "%s-task-%d-%d" format(config.getName.get, samzaTaskId, version)

  def getMesosTaskId: String = getMesosTaskName

  def getSamzaCommandBuilder: CommandBuilder = {
    val cmdBuilderClassName = config.getCommandClass.getOrElse(classOf[ShellCommandBuilder].getName)
    Class.forName(cmdBuilderClassName).newInstance.asInstanceOf[CommandBuilder]
      .setConfig(config)
      .setId(samzaTaskId)
      .setUrl(state.jobCoordinator.server.getUrl)
  }

  def getBuiltMesosCommandInfoURI: CommandInfo.URI = {
    val packagePath = {
      config.getPackagePath.get
    }
    CommandInfo.URI.newBuilder()
      .setValue(packagePath)
      .setExtract(true)
      .build()
  }

  def getBuiltMesosEnvironment(envMap: util.Map[String, String]): Environment = {
    val mesosEnvironmentBuilder: Environment.Builder = Environment.newBuilder()
    envMap foreach (kv => {
      mesosEnvironmentBuilder.addVariables(
        Environment.Variable.newBuilder()
          .setName(kv._1)
          .setValue(kv._2)
          .build()
      )
    })
    mesosEnvironmentBuilder.build()
  }

  def getBuiltMesosTaskID: TaskID = {
    TaskID.newBuilder()
      .setValue(getMesosTaskId)
      .build()
  }

  def getBuiltMesosCommandInfo: CommandInfo = {
    val samzaCommandBuilder = getSamzaCommandBuilder
    CommandInfo.newBuilder()
      .addUris(getBuiltMesosCommandInfoURI)
      .setValue(samzaCommandBuilder.buildCommand())
      .setEnvironment(getBuiltMesosEnvironment(samzaCommandBuilder.buildEnvironment()))
      .build()
  }

  def getBuiltMesosTaskInfo(slaveId: SlaveID): TaskInfo = {
    TaskInfo.newBuilder()
      .setTaskId(getBuiltMesosTaskID)
      .setSlaveId(slaveId)
      .setName(getMesosTaskName)
      .setCommand(getBuiltMesosCommandInfo)
      .addResources(
        Resource.newBuilder
          .setName("cpus")
          .setType(Value.Type.SCALAR)
          .setScalar(
            Value.Scalar.newBuilder().setValue(
              config.getExecutorMaxCpuCores
            )
          ).build()
      )
      .addResources(
        Resource.newBuilder
          .setName("mem")
          .setType(Value.Type.SCALAR)
          .setScalar(
            Value.Scalar.newBuilder().setValue(
              config.getExecutorMaxMemoryMb
            )
          ).build()
      )
      .addResources(
        Resource.newBuilder
          .setName("disk")
          .setType(Value.Type.SCALAR)
          .setScalar(
            Value.Scalar.newBuilder().setValue(
              config.getExecutorMaxDiskMb
            )
          ).build()
      ).build()
  }
}

