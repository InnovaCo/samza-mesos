package eu.inn.samza.mesos.allocation

import eu.inn.samza.mesos.SamzaSchedulerState.ScheduledTask
import eu.inn.samza.mesos.allocation.MesosOfferMapper.OfferResourceHolder
import org.apache.mesos.Protos.{Offer, SlaveID}

import scala.collection.JavaConversions._
import scala.collection.mutable

object MesosOfferMapper {

  case class OfferResourceHolder(offer: Offer) extends ResourceHolder {
    lazy val holderId = HolderId(offer.getSlaveId.getValue)

    lazy val resources: Map[String, Double] =
      offer.getResourcesList.filter(_.getRole == "*").map(r ⇒ r.getName → r.getScalar.getValue).toMap

    lazy val attributes: Map[String, String] =
      offer.getAttributesList.map(r ⇒ r.getName → r.getText.getValue).toMap
  }
}


class MesosOfferMapper(constraints: ResourceConstraints, strategy: ResourceMappingStrategy) {

  def mapResources(offers: List[Offer], tasks: Set[ScheduledTask], allocations: Map[SlaveID, Set[ScheduledTask]]): Map[Offer, Set[ScheduledTask]] = {

    val offersToTasks = mutable.Map(offers.map(_ → Set.empty[ScheduledTask]): _*)

    strategy
      .mapResources(offers.map(OfferResourceHolder), tasks, constraints, allocations.map(kv ⇒ HolderId(kv._1.getValue) → kv._2))
      .map(kv ⇒ kv._1.offer → kv._2)
      .foreach(kv ⇒ offersToTasks(kv._1) = kv._2)

    offersToTasks.toMap
  }
}
