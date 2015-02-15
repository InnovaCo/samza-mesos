package eu.inn.samza.mesos

import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.SerializableSerializer
import org.apache.samza.util.{ExponentialSleepStrategy, Logging}

import scala.collection.mutable
import scala.concurrent.duration._

object ReservationHint {
  def fromString(in: String) = {
    val Array(id, slaveId) = in.split("@", 2)
    ReservationHint(id, slaveId)
  }
}

case class ReservationHint(id: String, slaveId: String) {
  override def toString = s"$id@$slaveId"
}

object ZooRegistry {
  def apply(config: MesosConfig) =
    new ZooRegistry(config.getName.get, config.registryConnect, 30 seconds, 30 seconds)
}

//todo: abstract registry?
class ZooRegistry(jobName: String, connect: String, connectionTimeout: FiniteDuration, sessionTimeout: FiniteDuration) extends Logging {

  private val retryLoop =
    new ExponentialSleepStrategy(
      backOffMultiplier = 2.0,
      initialDelayMs = 10,
      maximumDelayMs = 1000
    )

  private def reservationPath(container: Int) =
    s"/jobs/$jobName/containers/$container/reservation"

  private def buildZkClient() =
    new ZkClient(connect, sessionTimeout.toMillis.toInt, connectionTimeout.toMillis.toInt, new SerializableSerializer())

  private val hints = mutable.Map.empty[Int, Option[ReservationHint]]

  def getReservationFor(container: Int): Option[ReservationHint] = {
    hints.getOrElseUpdate(container, {
      retryLoop.run[Option[ReservationHint]]({ loop ⇒
        val zk = buildZkClient()
        val path = reservationPath(container)
        try {
          debug(s"Reading from ZooKeeper: $path")
          zk.readData[String](path, true) match {
            case null ⇒
              debug(s"No value stored in ZooKeeper: $path")
              loop.done
              None
            case value ⇒
              val hint = ReservationHint.fromString(value)
              debug(s"Successfully read from ZooKeeper: $path = ${hint.toString}")
              loop.done
              Some(hint)
          }
        } finally {
          zk.close()
        }
      }, { (e, _) ⇒
        error("Unable to read from ZooKeeper.", e)
      }).flatten
    })
  }

  def setReservationFor(container: Int, hint: ReservationHint) = {
    retryLoop.run({ loop ⇒
      val zk = buildZkClient()
      val path = reservationPath(container)
      try {
        debug(s"Updating ZooKeeper: $path = ${hint.toString}")
        zk.createPersistent(path, true)
        zk.writeData(path, hint.toString)
        hints(container) = Some(hint)
        loop.done
      } finally {
        zk.close()
      }
    }, { (e, _) ⇒
      error("Unable to write to ZooKeeper.", e)
    })
  }
}
