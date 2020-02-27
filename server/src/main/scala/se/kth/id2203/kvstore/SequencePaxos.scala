package se.kth.id2203.kvstore

import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.sics.kompics.network._
import se.sics.kompics.sl._

import scala.collection.mutable;

class SequenceConsensus extends Port {
  request[SC_Propose];
  indication[SC_Decide];
  request[StartSequenceCons];
}

case class Prepare(nL: Long, ld: Int, na: Long) extends KompicsEvent;
case class Promise(nL: Long, na: Long, suffix: List[Op], ld: Int) extends KompicsEvent;
case class AcceptSync(nL: Long, suffix: List[Op], ld: Int) extends KompicsEvent;
case class Accept(nL: Long, c: Op) extends KompicsEvent;
case class Accepted(nL: Long, m: Int) extends KompicsEvent;
case class Decide(ld: Int, nL: Long) extends KompicsEvent;
case class StartSequenceCons(nodes: Set[NetAddress]) extends KompicsEvent;
case class SC_Propose(value: Op) extends KompicsEvent;
case class SC_Decide(value: Op) extends KompicsEvent;

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

  val las = mutable.Map.empty[NetAddress, Int];
  val lds = mutable.Map.empty[NetAddress, Int];
  val acks = mutable.Map.empty[NetAddress, (Long, List[Op])];
  var self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address");
  var pi: Set[NetAddress] = Set[NetAddress]()
  var others: Set[NetAddress] = Set[NetAddress]()
  var majority: Int = (pi.size / 2) + 1;
  var state: (Role.Value, State.Value) = (FOLLOWER, UNKOWN);
  var nL = 0L;
  var nProm = 0L;
  var leader: Option[NetAddress] = None;
  var na = 0L;
  var va = List.empty[Op];
  var ld = 0;
  // leader state
  var propCmds = List.empty[Op];
  var lc = 0;

  ble uponEvent {
    case BLE_Leader(l, n) => {
      if (n > nL) {
        leader = Some(l);
        nL = n;
        println(s"---------------------------")
        println(s"New leader has been elected: $leader");
        println(s"---------------------------")
        if (self == l && nL > nProm) {
          state = (LEADER, PREPARE);
          propCmds = List.empty[Op];
          for (p <- pi) {
            las += ((p, 0))
          }
          lds.clear;
          acks.clear;
          lc = 0;
          for (p <- pi - self) {
            trigger(NetMessage(self, p, Prepare(nL, ld, na)) -> pl);
          }
          acks += ((l, (na, suffix(va, ld))))
          lds += ((self, ld))
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
        var sfx = List.empty[Op];
        if (na >= n) {
          sfx = suffix(va, ldp);
        }
        trigger(NetMessage(p.dst, p.src, Promise(np, na, sfx, ldp)) -> pl);
      }
    }
    case NetMessage(a, Promise(n, na, sfxa, lda)) => {
      if ((n == nL) && (state == (LEADER, PREPARE))) {
        acks += ((a.src, (na, sfxa)))
        lds += ((a.src, lda))
        val P = pi.filter(acks isDefinedAt _);
        val Psize = P.size;
        if (P.size == majority) {
          val (k, sfx) = acks.maxBy{case (_, (v, _)) => v };
          va = prefix(va, ld) ++ sfx._2 ++ propCmds;
          las(self) = va.size
          propCmds = List.empty[Op];
          state = (LEADER, ACCEPT);
          for (p <- pi) {
            if ((lds isDefinedAt p) && p != self) {
              val sfxp = suffix(va, lds(p));
              trigger(NetMessage(self, p, AcceptSync(nL, sfxp, lds(p))) -> pl);
            }
          }
        }
      } else if ((n == nL) && (state == (LEADER, ACCEPT))) {
        lds(a.src) = lda;
        var sfx = suffix(va, lds(a.src));
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
    case NetMessage(p, Accept(nL, c: Op)) => {
      if ((nProm == nL) && (state == (FOLLOWER, ACCEPT))) {
        va :+= c;
        trigger(NetMessage(self, p.src, Accepted(nL, va.size)) -> pl);
      }
    }
    case NetMessage(h, Decide(l, nL)) => {
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
        var P = pi.filter(las isDefinedAt _).filter(las(_) >= m);
        if (lc < m && P.size >= majority) {
          lc = m;
          for (p <- pi) {
            if (lds isDefinedAt p) {
              trigger(NetMessage(self, p, Decide(lc, nL)) -> pl);
            }
          }
        }
      }
    }
  }

  sc uponEvent {
    case SC_Propose(c: Op) => {
      if (state == (LEADER, PREPARE)) {
        propCmds :+= c;
      }
      else if (state == (LEADER, ACCEPT)) {
        va :+= c;
        las(self) += 1;
        for (p <- pi) {
          if ((lds isDefinedAt p) && p != self) {
            trigger(NetMessage(self, p, Accept(nL, c)) -> pl);
          }
        }
      }
    }
    case StartSequenceCons(nodes: Set[NetAddress]) => {
      pi = nodes;
      majority = pi.size / 2 + 1;
      trigger(StartElection(nodes) -> ble);
    }
  }

  def suffix(s: List[Op], l: Int): List[Op] = {
    s.drop(l)
  }

  def prefix(s: List[Op], l: Int): List[Op] = {
    s.take(l)
  }
}