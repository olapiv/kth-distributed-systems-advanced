package se.kth.id2203.kvstore

import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.kth.id2203.overlay.LookupTable
import se.sics.kompics.network._
import se.sics.kompics.sl.{Init, _}
import se.sics.kompics.timer.{ScheduleTimeout, Timer}
import se.sics.kompics.{ComponentDefinition => _, Port => _}

case class Suspect(process: NetAddress) extends KompicsEvent;
case class Restore(process: NetAddress) extends KompicsEvent;
case class StartDetector(lookupTable: Option[LookupTable], nodes: Set[NetAddress]) extends KompicsEvent;

case class HeartbeatReply(seq: Int) extends KompicsEvent;
case class HeartbeatRequest(seq: Int) extends KompicsEvent;

class EventuallyPerfectFailureDetector extends Port {
  indication[Suspect];
  indication[Restore];
  request[StartDetector];
}

class EPFD extends ComponentDefinition {

  val timer = requires[Timer];
  val pLink = requires[Network];
  val epfd = provides[EventuallyPerfectFailureDetector];

  val self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address");
  var currentTopology: List[NetAddress] = List.empty;
  var systemTopology: Option[LookupTable] = None

  val delta = 2000;
  var period = 2000;

  var alive = Set[NetAddress]();
  var suspected = Set[NetAddress]();
  var seqnum = 0;
  var debugSource: Option[NetAddress] = None

  def startTimer(delay: Long): Unit = {
    val scheduledTimeout = new ScheduleTimeout(period);
    scheduledTimeout.setTimeoutEvent(CheckTimeout(scheduledTimeout));
    trigger(scheduledTimeout -> timer);
  }

  timer uponEvent {
    case CheckTimeout(_) => {
      if (alive.intersect(suspected).nonEmpty) {
        period += delta;
      }
      seqnum = seqnum + 1;
      for (p <- currentTopology) {
        if (!alive.contains(p) && !suspected.contains(p)) {
          suspected += p;
          println(s"$self EFPD suspected $p")
          trigger(Suspect(p) -> epfd)
          if(debugSource.isDefined)
            trigger(NetMessage(self, debugSource.get, Suspect(p)) -> pLink)
        } else if (alive.contains(p) && suspected.contains(p)) {
          suspected = suspected - p;
            println(s"$self EFPD restored $p")
            trigger(Restore(p) -> epfd);
        }
          trigger(NetMessage(self, p, HeartbeatRequest(seqnum)) -> pLink);
      }
      alive = Set[NetAddress]();
      startTimer(period);
    }
  }

  pLink uponEvent {
    case NetMessage(src, HeartbeatRequest(seq)) => {
      trigger(NetMessage(self, src.src, HeartbeatReply(seq)) -> pLink);
    }
    case NetMessage(src, HeartbeatReply(seq)) => {
      if (seq == seqnum || suspected.contains(src.src))
        alive = alive + src.src;
    }
  }

  epfd uponEvent {
    case StartDetector(lookupTable: Option[LookupTable], nodes: Set[NetAddress]) => {
      systemTopology = lookupTable
      currentTopology = nodes.toList;
      suspected = Set[NetAddress]();
      alive = currentTopology.toSet
      startTimer(period);
    }
  }

};
