package se.kth.id2203.kvstore

import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.kth.id2203.overlay.LookupTable
import se.sics.kompics.KompicsEvent
import se.sics.kompics.network.Network
import se.sics.kompics.sl.{ComponentDefinition, Init, NegativePort, Port, PositivePort}

import scala.collection.immutable.Set

case class BEB_Deliver(src: NetAddress, payload: KompicsEvent) extends KompicsEvent;
case class BEB_Broadcast(payload: KompicsEvent) extends KompicsEvent;
case class BEB_Broadcast_Global(payload: KompicsEvent) extends KompicsEvent
case class SetTopology(lut: Option[LookupTable], nodes: Set[NetAddress]) extends KompicsEvent;

class BestEffortBroadcast extends Port {
  indication[BEB_Deliver];
  request[BEB_Broadcast];
  request[BEB_Broadcast_Global]
}

class BasicBroadcast(bebInit: Init[BasicBroadcast]) extends ComponentDefinition {
  val pLink: PositivePort[Network] = requires[Network];
  var beb: NegativePort[BestEffortBroadcast] = provides[BestEffortBroadcast];

  var self: NetAddress = bebInit match {
    case Init(s: NetAddress) => s
  };
  var myPartitionTopology: List[NetAddress] = List.empty;
  var systemTopology: Option[LookupTable] = None

  beb uponEvent {
    case x: BEB_Broadcast_Global => {
      for (it <- systemTopology.get.partitions; address <- it._2)
        trigger(NetMessage(self, address, x) -> pLink)
    }

    case x: BEB_Broadcast => {
      for (q <- myPartitionTopology) {
        trigger(NetMessage(self, q, x) -> pLink);
      }
    }

    case SetTopology(lookupTable: Option[LookupTable], nodes: Set[NetAddress]) => {
      systemTopology = lookupTable
      myPartitionTopology = nodes.toList;
    }
  }

  pLink uponEvent {
    case NetMessage(_, BEB_Broadcast_Global(deb@Debug("BroadcastFlood", receiver, _))) => {
      trigger(NetMessage(self, receiver, BEB_Deliver(receiver, deb)) -> pLink)
    }
    case NetMessage(_, BEB_Broadcast_Global(dbm@Debug("FailureDetect", _, _))) => {
      trigger(NetMessage(self, self, dbm) -> pLink)
    }

    case NetMessage(src, BEB_Broadcast_Global(payload)) => {
      trigger(BEB_Deliver(src.src, payload) -> beb);
    }
    case NetMessage(src, BEB_Broadcast(payload)) => {
      trigger(BEB_Deliver(src.src, payload) -> beb);
    }
  }
}

