package se.kth.id2203.kvstore

import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.sics.kompics.sl._
import se.sics.kompics.network._
import se.sics.kompics.KompicsEvent

import scala.collection.mutable

class SequenceConsensus extends Port {
  request[SC_Propose]
  indication[SC_Decide]
  request[StartSequenceCons]
}

case class SC_Propose(value: Operation) extends KompicsEvent
case class SC_Decide(value: Operation) extends KompicsEvent
case class StartSequenceCons(nodes: Set[NetAddress]) extends KompicsEvent

case class Prepare(nL: Long, ld: Int, na: Long) extends KompicsEvent
case class Promise(nL: Long, na: Long, suffix: List[Operation], ld: Int) extends KompicsEvent
case class AcceptSync(nL: Long, suffix: List[Operation], ld: Int) extends KompicsEvent
case class Accept(nL: Long, c: Operation) extends KompicsEvent
case class Accepted(nl: Long, m: Int) extends KompicsEvent
case class Decide(ld: Int, nL: Long) extends KompicsEvent

object State extends Enumeration {
  type State = Value;
  val PREPARE, ACCEPT, UNKOWN = Value;
}

object Role extends Enumeration {
  type Role = Value;
  val LEADER, FOLLOWER = Value;
}

class SequencePaxos extends ComponentDefinition {

  import Role._
  import State._

  val sc = provides[SequenceConsensus];
  val ble = requires[BallotLeaderElection];
  val pl = requires[Network];

//  val (self, pi, others) = init match {
//    case Init(addr: Address, pi: Set[Address] @unchecked) => (addr, pi, pi - addr)
//  }
  var pi: Set[NetAddress] = Set[NetAddress]()
  var others: Set[NetAddress] = Set[NetAddress]()
  var majority = (pi.size / 2) + 1;

  var state = (FOLLOWER, UNKOWN); // COuld be broken
  var nL = 0l;
  var nProm = 0l;
  var leader: Option[NetAddress] = None;
  var na = 0l;
  var va = List.empty[Operation];
  var ld = 0;
  // leader state
  var propCmds = List.empty[Operation];
  val las = mutable.Map.empty[NetAddress, Int];
  val lds = mutable.Map.empty[NetAddress, Int];
  var lc = 0;
  val acks = mutable.Map.empty[NetAddress, (Long, List[Operation])];
  val self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address")

  private def maximum( map: mutable.Map[NetAddress, (Long, List[Operation])] ): (Long, List[Operation]) = {
    var highestRound: Long = 0;
    var v = List.empty[Operation]
    for ((_, tuple) <- map) {
      if( tuple._1 > highestRound ) {
        highestRound = tuple._1
        v = tuple._2
      } else if ( (tuple._1 == highestRound ) && ( tuple._2.size > v.size ) ) {
        highestRound = tuple._1
        v = tuple._2
      }
    }
    (highestRound, v)
  }


  ble uponEvent {
    case BLE_Leader(l, n) => {
      /* INSERT YOUR CODE HERE */
      if (n > nL)  {
        leader = Some(l);
        nL = n;
        if ((self == l) && nL > nProm) {
          state = (LEADER, PREPARE);
          propCmds = List.empty[Operation];
          for (p <- pi) {
            las += ((p, 0));
          }
          lds.clear;
          acks.clear;
          lc = 0;

          for (p <- others) {
            trigger(NetMessage(self, p, Prepare(nL, ld, na)) -> pl);
          }
          acks += ((l, (na, suffix(va, ld))));
          lds += ((self, ld));
          nProm = nL;
        } else {
          state = (FOLLOWER, state._2);
        }
      }
    }
  }

  pl uponEvent {
    case NetMessage(p, Prepare(np, ldp, n)) => {
      if (nProm < np) {
        nProm = np;
        state = (FOLLOWER, PREPARE);
        var sfx: List[Operation] = List.empty[Operation];
        if (na < n) {
          sfx = suffix(va, ld);
        }
        trigger(NetMessage(self, p.src, Promise(np, na, sfx, ld)) -> pl);
      }
    }
    case NetMessage(a, Promise(n, na, sfxa, lda)) => {
      if ((n == nL) && (state == (LEADER, PREPARE))) {
        acks += ((a, (na, sfxa)));
        lds += ((a, lda));
        if (acks.size == majority) {
          val (k, sfx) = maximum(acks);
          va = prefix(va, ld) ++ sfx ++ propCmds;
          las(self) = va.size;
          propCmds = List.empty[Operation];
          state = (LEADER, ACCEPT);
          for ((p, v) <- lds) {
            val sfxp = suffix(va, v);
            trigger(NetMessage(self, p, AcceptSync(nL, sfxp, v)) -> pl);
          }
        }
      } else if ((n == nL) && (state == (LEADER, ACCEPT))) {
        lds += ((a, lda));
        val sfx = suffix(va, lds(a.src));
        //val sfx = suffix(va, lds(a));
        trigger(NetMessage(self, a.src, AcceptSync(nL, sfx, lds(a.src))) -> pl);
        if (lc != 0) {
          trigger(NetMessage(self, a.src, Decide(ld, nL)) -> pl);
        }
      }
    }
    case NetMessage(p, AcceptSync(nL, sfx, ldp)) => {
      if ((nProm == nL) && (state == (FOLLOWER, PREPARE))) {
        na = nL;
        va = prefix(va, ldp) ++ sfx;
        trigger(NetMessage(self, p.src, Accepted(nL, va.size)) -> pl);
        state = (FOLLOWER, ACCEPT);
      }
    }
    case NetMessage(p, Accept(nL, c)) => {
      if ((nProm == nL) && (state == (FOLLOWER, ACCEPT))) {
        va = va :+ c;
        trigger(NetMessage(self, p.src, Accepted(nL, va.size)) -> pl);
      }
    }
    case NetMessage(_, Decide(l, nL)) => {
      if (nProm == nL) {
        while (ld < l) {
          trigger(SC_Decide(va(ld)) -> sc);
          ld += 1;
        }
      }
    }
    case NetMessage(a, Accepted(n, m)) => {
      if ((n == nL) && (state == (LEADER, ACCEPT))) {
        las(a.src) = m;
        if ((lc < m) && (pi.filter(p => las(p) >= m).size >= majority)) { // Maybe broken
          lc = m;
          for ((p, v) <- lds) {
            trigger(NetMessage(self, p, Decide(lc, nL)) -> pl);
          }
        }
      }
    }
  }

  sc uponEvent {
    case SC_Propose(c) => {
      if (state == (LEADER, PREPARE)) {
        propCmds = propCmds :+ c;
      }
      else if (state == (LEADER, ACCEPT)) {
        va = va :+ c;
        las(self) += 1;
        // las(self) = las(self) + 1;
        for ((p, v) <- lds) {
          if (p != self) {
            trigger(NetMessage(self, p, Accept(nL, c)) -> pl);
          }
        }
      }
    }
    case StartSequenceCons(nodes: Set[NetAddress]) => {
      pi = nodes
      majority = pi.size / 2 + 1
      trigger(StartElection(nodes) -> ble)
    }
  }

  def suffix(s: List[Operation], l: Int): List[Operation] = {
    s.drop(l)
  }

  def prefix(s: List[Operation], l: Int): List[Operation] = {
    s.take(l)
  }
}