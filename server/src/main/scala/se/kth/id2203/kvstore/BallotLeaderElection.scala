package se.kth.id2203.kvstore

import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.sics.kompics.network._
import se.sics.kompics.sl._
import se.sics.kompics.timer.{ScheduleTimeout, Timeout, Timer}
import se.sics.kompics.KompicsEvent

import scala.collection.mutable;

class BallotLeaderElection extends Port {
  indication[BLE_Leader];
}
case class BLE_Leader(leader: NetAddress, ballot: Long) extends KompicsEvent;


case class CheckTimeout(timeout: ScheduleTimeout) extends Timeout(timeout);
case class HeartbeatReq(round: Long, highestBallot: Long) extends KompicsEvent;
case class HeartbeatResp(round: Long, ballot: Long) extends KompicsEvent;
case class StartElection(nodes: Set[NetAddress]) extends KompicsEvent

class GossipLeaderElection extends ComponentDefinition {

  val ble = provides[BallotLeaderElection];
  val pl = requires[Network];
  val timer = requires[Timer];

  private val ballotOne = 0x0100000000L;
  val self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address")
  var topology: Set[NetAddress] = Set.empty;
  val delta = 1000;
  var majority = 0;

  private var period = delta;
  private val ballots = mutable.Map.empty[NetAddress, Long];

  private var round = 0L;
  private var ballot = ballotFromNAddress(0, self);

  private var leader: Option[(Long, NetAddress)] = None;
  private var highestBallot: Long = ballot;

  def ballotFromNAddress(n: Int, adr: NetAddress): Long = {
    val nBytes = com.google.common.primitives.Ints.toByteArray(n);
    val addrBytes = com.google.common.primitives.Ints.toByteArray(adr.hashCode());
    val bytes = nBytes ++ addrBytes;
    val r = com.google.common.primitives.Longs.fromByteArray(bytes);
    assert(r > 0); // should not produce negative numbers!
    r
  }

  def incrementBallotBy(ballot: Long, inc: Long): Long = {
    ballot + inc * ballotOne
  }

  private def incrementBallot(ballot: Long): Long = {
    ballot + ballotOne
  }

  private def startTimer(delay: Long): Unit = {
    val scheduledTimeout = new ScheduleTimeout(period);
    scheduledTimeout.setTimeoutEvent(CheckTimeout(scheduledTimeout));
    trigger(scheduledTimeout -> timer);
  }

  private def checkLeader(): Unit = {
    ballots += ((self, ballot));
    val top = ballots.maxBy(_._2);
    val (topProcess, topBallot) = top;

    if (topBallot < highestBallot) {
      ballot = incrementBallotBy(ballot, topBallot - ballot + 1)
      leader = None

    } else {
      if (leader.isDefined) {
        if ((topBallot, topProcess) != leader.get) {
          highestBallot = topBallot;
          leader = Some((topBallot, topProcess));
          trigger(BLE_Leader(topProcess, topBallot) -> ble);
        }
      } else {
        highestBallot = topBallot;
        leader = Some((topBallot, topProcess));
        trigger(BLE_Leader(topProcess, topBallot) -> ble);
      }
    }
  }

  timer uponEvent {
    case CheckTimeout(_) => {
      if (ballots.size + 1 >= majority) {
        checkLeader();
      }
      ballots.clear
      round += 1;
      for (p <- topology) {
        if (p != self) {
          trigger(NetMessage(self, p, HeartbeatReq(round, highestBallot)) -> pl);
        }
      }
      startTimer(period);
    }
  }

  pl uponEvent {
    case NetMessage(src, HeartbeatReq(r, hb)) => {
      if (hb > highestBallot) {
        highestBallot = hb;
      }
      trigger(NetMessage(self, src.src, HeartbeatResp(r, ballot)) -> pl);
    }
    case NetMessage(src, HeartbeatResp(r, b)) => {
      if (r == round) {
        ballots += ((src.src, b));
      } else {
        period += delta;
      }
    }
  }

  ble uponEvent {
    case StartElection(nodes: Set[NetAddress]) => {
      topology = nodes
      majority = topology.size / 2 + 1
      startTimer(period)
    }
  }
}