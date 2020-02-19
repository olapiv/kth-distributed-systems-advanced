package se.kth.id2203.kvstore

import com.larskroll.common.collections.TreeSetMultiMap
import se.kth.id2203.networking.{NetAddress, NetHeader, NetMessage}
import se.sics.kompics.network.Network
import se.sics.kompics.sl.{ComponentDefinition, KompicsEvent, Port}

import scala.collection.mutable

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
case class Decided(key: Int, decidedValue: Any) extends KompicsEvent

class Paxos(val numProcesses: Int) extends ComponentDefinition {

  // TODO: Add addKey function to instantiate default values

  val consensus = provides[Consensus]
  val beb = requires[BebPort]
  val net = requires[Network]

  val self = cfg.getValue[NetAddress]("id2203.project.address")

  var timestamp = mutable.Map.empty[Int, Int]  // [key, timestamp]
  var proposedValue = mutable.Map.empty[Int, Option[Any]]
  var promises = Map.empty[Int, mutable.ListBuffer[(Int, Option[Any])]]

  var numOfAccepts = mutable.Map.empty[Int, Int]
  var decided = mutable.Map.empty[Int, Boolean];  // should default to false

  // Acceptor State
  var promisedTimestamp = mutable.Map.empty[Int, Int];  // should default to 0
  var acceptedTimestamp = mutable.Map.empty[Int, Int];  // should default to 0
  var acceptedValue = mutable.Map.empty[Int, Option[Any]];

  def propose(key: Int) = {
    if (!decided(key)) {
      timestamp(key) += 1;
      numOfAccepts(key) = 0;
      promises(key).clear();
      trigger(BebBroadcast(Prepare(key, timestamp(key))) -> beb);
    }
  }

  consensus uponEvent {
    case Propose(key, value) => {
      proposedValue(key) = Some(value);
      propose(key);
    }
  }


  beb uponEvent {
    case BebDeliver(src, prep: Prepare) => {
      if (promisedTimestamp(prep.key) < prep.timestamp) {
        promisedTimestamp(prep.key) = prep.timestamp;
        trigger(NetMessage(self, src, Promise(prep.key, promisedTimestamp(prep.key), acceptedTimestamp(prep.key), acceptedValue(prep.key))) -> net);
      } else {
        trigger(NetMessage(self, src, Nack(prep.key, prep.timestamp)) -> net);
      }
    };

    case BebDeliver(src, acc: Accept) => {
      if (promisedTimestamp(acc.key) <= acc.timestamp) {
        promisedTimestamp(acc.key) = acc.timestamp;
        acceptedTimestamp(acc.key) = acc.timestamp;
        acceptedValue(acc.key) = Some(acc.proposedValue);
        trigger(NetMessage(self, src, Accepted(acc.key, acc.timestamp)) -> net)
      } else {
        trigger(NetMessage(self, src, Nack(acc.key, acc.timestamp)) -> net)
      }
    };

    case BebDeliver(src, dec : Decided) => {
      if (!decided(dec.key)) {
        trigger(Decide(dec.key, dec.decidedValue) -> consensus);
        decided(dec.key) = true;
      }
    }
  }


  net uponEvent {

    case NetMessage(NetHeader(src, _, _), prepAck: Promise) =>
      if (timestamp(prepAck.key) == prepAck.timestamp) {
        promises(prepAck.key) += ((prepAck.acceptedTimestamp, prepAck.acceptedValue));
        if(promises(prepAck.key).size == math.ceil( numProcesses+1 / 2).toInt) {
          var highestBallotPromise = (promises(prepAck.key).maxBy { case (timeSt, accV) => timeSt });
          if(highestBallotPromise._2.isDefined) {
            proposedValue(prepAck.key) = highestBallotPromise._2;
          }
          trigger(BebBroadcast(Accept(prepAck.key, timestamp(prepAck.key), proposedValue(prepAck.key))) -> beb);
        }
      }

    case NetMessage(NetHeader(src, _, _), accAck: Accepted) =>
      if (timestamp(accAck.key) == accAck.timestamp) {
        numOfAccepts(accAck.key) += 1;
        if(numOfAccepts(accAck.key) == math.ceil( numProcesses + 1 / 2).toInt) {
          trigger(BebBroadcast(Decided(accAck.key, proposedValue(accAck.key))) -> beb);
        }
      }

    case NetMessage(NetHeader(src, _, _), nack: Nack) =>
      if (timestamp(nack.key) == nack.timestamp) {
        propose(nack.key);
      }
  }
}


