package se.kth.id2203.kvstore

import java.util.UUID

import se.kth.id2203.networking.{NetAddress, NetHeader, NetMessage}
import se.sics.kompics.network.Network
import se.sics.kompics.sl.{ComponentDefinition, PositivePort, NegativePort}
import se.sics.kompics.KompicsEvent
import se.sics.kompics.PortType
import scala.collection.immutable.Set

case class BebBroadcast(payload: KompicsEvent) extends KompicsEvent with Serializable {
  val uuid: UUID = UUID.randomUUID()
}
case class BebDeliver(src: NetAddress, event: KompicsEvent) extends KompicsEvent with Serializable
case class BebTopology(src: Set[NetAddress]) extends KompicsEvent with Serializable

class BebPort extends PortType {
  request(classOf[BebBroadcast])
  request(classOf[BebTopology])
  indication(classOf[BebDeliver])
}

class Beb extends ComponentDefinition {
  val net: PositivePort[Network] = requires[Network]
  val beb: NegativePort[BebPort] = provides[BebPort]
  private var allAddresses: Set[NetAddress] = Set.empty;
  private val self = cfg.getValue[NetAddress]("id2203.project.address")

  beb uponEvent {
    case BebBroadcast(payload) =>
      for (a <- allAddresses) {
        trigger(NetMessage(self, a, payload) -> net)
      }
    case BebTopology(addresses) =>
      allAddresses = addresses
  }

  net uponEvent {
    case NetMessage(NetHeader(src, _, _), payload) =>
      trigger(BebDeliver(src, payload) -> beb)
  }
}