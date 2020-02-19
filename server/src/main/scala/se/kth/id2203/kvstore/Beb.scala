package se.kth.id2203.kvstore

import java.util.UUID

import se.kth.id2203.networking.{NetAddress, NetHeader, NetMessage}
import se.sics.kompics.network.Network
import se.sics.kompics.sl.{ComponentDefinition, Port}
import se.sics.kompics.KompicsEvent

case class BebRequest(payload: KompicsEvent, addresses: List[NetAddress]) extends KompicsEvent with Serializable {
  val uuid: UUID = UUID.randomUUID()
}
case class BebDeliver(src: NetAddress, event: KompicsEvent) extends KompicsEvent with Serializable

class BebPort extends Port {
  request(classOf[BebRequest])
  indication(classOf[BebDeliver])
}

class Beb extends ComponentDefinition {
  val net = requires[Network]
  val beb = provides[BebPort]
  val self = cfg.getValue[NetAddress]("id2203.project.address")

  beb uponEvent {
    case BebRequest(payload, addresses) =>
      for (a <- addresses) {
        trigger(NetMessage(self, a, payload) -> net)
      }
  }

  net uponEvent {
    case NetMessage(NetHeader(src, _, _), payload) =>
      trigger(BebDeliver(src, payload) -> beb)
  }
}