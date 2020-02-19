package se.kth.id2203.kvstore

import java.util.UUID

import se.kth.id2203.networking.{NetAddress, NetHeader, NetMessage}
import se.sics.kompics.network.Network
import se.sics.kompics.sl.{ComponentDefinition, PositivePort, NegativePort}
import se.sics.kompics.KompicsEvent
import se.sics.kompics.PortType

case class BebBroadcast(payload: KompicsEvent) extends KompicsEvent with Serializable {
  val uuid: UUID = UUID.randomUUID()
}
case class BebDeliver(src: NetAddress, event: KompicsEvent) extends KompicsEvent with Serializable

class BebPort extends PortType {
  request(classOf[BebBroadcast])
  indication(classOf[BebDeliver])
}

class Beb(allAdresses: List[NetAddress]) extends ComponentDefinition {
  val net: PositivePort[Network] = requires[Network]
  val beb: NegativePort[BebPort] = provides[BebPort]
  val self = cfg.getValue[NetAddress]("id2203.project.address")

  beb uponEvent {
    case BebBroadcast(payload) =>
      for (a <- allAdresses) {
        trigger(NetMessage(self, a, payload) -> net)
      }
  }

  net uponEvent {
    case NetMessage(NetHeader(src, _, _), payload) =>
      trigger(BebDeliver(src, payload) -> beb)
  }
}