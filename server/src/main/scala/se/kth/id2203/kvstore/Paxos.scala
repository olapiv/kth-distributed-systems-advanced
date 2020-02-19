package se.kth.id2203.kvstore

import com.larskroll.common.collections.TreeSetMultiMap
import se.kth.id2203.networking.NetAddress
import se.sics.kompics.network.Network
import se.sics.kompics.sl.{ComponentDefinition, KompicsEvent, Port}

import scala.collection.mutable.ListBuffer

case class Propose(key: Int, value: Any) extends KompicsEvent
case class Decide(key: Int, value: Any) extends KompicsEvent
class Consensus extends Port {
  request[Propose]
  indication[Decide]
}

case class Prepare(key: Int, timestamp: Int) extends KompicsEvent
case class Promise(key: Int, timestamp: Int, acceptedTimestamp: Int, acceptedValue: Option[Any]) extends KompicsEvent
case class Accept(key: Int, timestamp: Int, proposedValue: Any) extends KompicsEvent
case class Accepted(key: Int, timestamp: Int) extends KompicsEvent
case class Nack(key: Int, timestamp: Int) extends KompicsEvent
case class Decided(decidedValue: Any) extends KompicsEvent

class Paxos(val numOfProcesses: Int) extends ComponentDefinition {

  val consensus = provides[Consensus]
  val beb = requires[Beb]
  val net = requires[Network]

  var timestamp = 0
  var numOfAccepts = 0
  var proposedValue: Option[Any] = None
  var promises = TreeSetMultiMap.empty[Int, (Int, Option[Any])]

}


