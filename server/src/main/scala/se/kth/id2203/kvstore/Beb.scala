package se.kth.id2203.kvstore

import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.kth.id2203.overlay.LookupTable
import se.sics.kompics.KompicsEvent
import se.sics.kompics.network.Network
import se.sics.kompics.sl.{ComponentDefinition, Init, NegativePort, Port, PositivePort}

import scala.collection.immutable.Set

case class BEB_Deliver(source: NetAddress, payload: KompicsEvent) extends KompicsEvent;
case class BEB_Broadcast(payload: KompicsEvent) extends KompicsEvent;
case class BEB_Topology(nodes: Set[NetAddress]) extends KompicsEvent;

class BestEffortBroadcast extends Port {
  indication[BEB_Deliver];
  request[BEB_Broadcast];
}

class BasicBroadcast extends ComponentDefinition {
  val pLink = requires[Network];
  var beb = provides[BestEffortBroadcast];
  val self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address")
  var nodes: Set[NetAddress] = Set.empty;

  beb uponEvent {
    case x: BEB_Broadcast => {
      for (q <- nodes) {
        trigger(NetMessage(self, q, x) -> pLink);
      }
    }

    case BEB_Topology(nodes: Set[NetAddress]) => {
      this.nodes = nodes
    }
  }

  pLink uponEvent {
    case NetMessage(src, BEB_Broadcast(payload)) => {
      trigger(BEB_Deliver(src.src, payload) -> beb);
    }
  }
}

