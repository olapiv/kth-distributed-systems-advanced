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

//Define EPFD Implementation
class EPFD(epfdInit: Init[EPFD]) extends ComponentDefinition {

  //EPFD subscriptions
  val timer = requires[Timer];
  val pLink = requires[Network];
  val epfd = provides[EventuallyPerfectFailureDetector];

  // EPDF component state and initialization

  //configuration parameters
  val self = epfdInit match {case Init(s: NetAddress) => s};
  var myPartitionTopology: List[NetAddress] = List.empty;
  var systemTopology: Option[LookupTable] = None

  val delta = cfg.getValue[Long]("id2203.project.failureDetectorInterval");
  //mutable state
  var period = cfg.getValue[Long]("id2203.project.failureDetectorInterval");
  var alive = Set[NetAddress]();
  var suspected = Set[NetAddress]();
  var seqnum = 0;
  var debugSource : Option[NetAddress] = None
  var responding : Boolean = true

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
      for (p <- myPartitionTopology) {
        if (!alive.contains(p) && !suspected.contains(p)) {
          suspected += p;

          if(responding){
            println(s"$self EFPD suspected $p")
            trigger(Suspect(p) -> epfd)
            // send suspect to debug source
            if(debugSource.isDefined)
              trigger(NetMessage(self, debugSource.get, Suspect(p)) -> pLink)
          }
        } else if (alive.contains(p) && suspected.contains(p)) {
          suspected = suspected - p;
          if(responding){
            println(s"$self EFPD restored $p")
            trigger(Restore(p) -> epfd);
          }
        }
        if(responding){
          trigger(NetMessage(self, p, HeartbeatRequest(seqnum)) -> pLink);
        }

      }
      alive = Set[NetAddress]();
      startTimer(period);
    }
  }

  pLink uponEvent {

    case NetMessage(src, deb @Debug("Suicide",_,_)) => {
      responding = false
      println(src)
      println(deb)
      println(self + " WILL STOP RESPONDING...")
      println()
      println()
    }
    case NetMessage(_, Debug("FailureDetect", source, _)) => {
      debugSource = Some(source)
    }

    case NetMessage(src, HeartbeatRequest(seq)) if responding => {
      trigger(NetMessage(self, src.src, HeartbeatReply(seq)) -> pLink);
    }
    case NetMessage(src, HeartbeatReply(seq)) => {
      if (seq == seqnum || suspected.contains(src.src))
        alive = alive + src.src;
    }
  }

  epfd uponEvent {
    case StartDetector(lookupTable: Option[LookupTable], nodes: Set[NetAddress]) => {
      // start detector for the nodes in particular partition
      systemTopology = lookupTable
      myPartitionTopology = nodes.toList;
      suspected = Set[NetAddress]();
      alive = myPartitionTopology.toSet
      startTimer(period);
    }
  }

};
