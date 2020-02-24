/*
 * The MIT License
 *
 * Copyright 2017 Lars Kroll <lkroll@kth.se>.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package se.kth.id2203.overlay;

import se.kth.id2203.kvstore._
import se.kth.id2203.bootstrapping._
import se.kth.id2203.networking._
import se.sics.kompics.network.Network
import se.sics.kompics.sl._
import se.sics.kompics.timer.Timer
//case class Debug(key: String, source: NetAddress, id: UUID = UUID.randomUUID()) extends Op

// import util.Random
// import se.kth.id2203.kvstore.{Beb, BebBroadcast, BebPort, BebTopology, SC_Topology, SequenceConsensus}
// import se.kth.id2203.bootstrapping._;

/**
  * The V(ery)S(imple)OverlayManager.
  * <p>
  * Keeps all nodes in a single partition in one replication group.
  * <p>
  * Note: This implementation does not fulfill the project task. You have to
  * support multiple partitions!
  * <p>
  * @author Lars Kroll <lkroll@kth.se>
  */
class VSOverlayManager extends ComponentDefinition {

  //******* Ports ******
  // val route = provides(Routing);
  // val boot = requires(Bootstrapping);
  // val net = requires[Network];
  // val timer = requires[Timer];

  // val beb = requires[BebPort];
  // val consensus = requires[SequenceConsensus]

  val route: NegativePort[Routing.type] = provides(Routing)
  val boot: PositivePort[Bootstrapping.type] = requires(Bootstrapping)
  val net: PositivePort[Network] = requires[Network]
  val timer: PositivePort[Timer] = requires[Timer]
  val epfd: PositivePort[EventuallyPerfectFailureDetector] = requires[EventuallyPerfectFailureDetector]
  val beb: PositivePort[BestEffortBroadcast] = requires[BestEffortBroadcast]

  //******* Fields ******
  // val self = cfg.getValue[NetAddress]("id2203.project.address");
  // private var lut: Option[LookupTable] = None;

  val self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address")
  var seqCons: PositivePort[SequenceConsensus] = requires[SequenceConsensus]
  var suspected = Set[NetAddress]()
  private var lut: Option[LookupTable] = None

  //******* Handlers ******

  beb uponEvent {
    case BEB_Deliver(src, payload) => handle {
      println(s"(BEB) Received broadcast from $src with $payload");
      trigger(NetMessage(src, self, payload) -> net);
    }
  }

  epfd uponEvent {
    case Suspect(p) => handle {
      suspected += p
    }
    case Restore(p) => handle {
      suspected -= p
    }
  }

  boot uponEvent {
    case GetInitialAssignments(nodes) => {
      log.info("Generating LookupTable...");
      val delta = cfg.getValue[Int]("id2203.project.delta")
      val lut = LookupTable.generate(nodes, delta);
      logger.debug("Generated assignments:\n$lut");
      trigger(new InitialAssignments(lut) -> boot);
    }
    case Booted(assignment: LookupTable) => {
      log.info("Got NodeAssignment, overlay ready.");
      lut = Some(assignment);
      // trigger(BebTopology(assignment.getNodes()) -> beb)
      // trigger(SC_Topology(assignment.getNodes()) -> consensus)
      val myPartitionTuple = assignment.partitions.find(_._2.exists(_.equals(self)))
      myPartitionTuple match {
        case Some((_, myPartition)) =>
          trigger(StartDetector(lut, myPartition.toSet) -> epfd)
          trigger(SetTopology(lut, myPartition.toSet) -> beb)
          trigger(StartSequenceCons(myPartition.toSet) -> seqCons)
        case None =>
          println("Could not find my partition.")
          throw new Exception(self + " Could not find its own partition in lookup table!")
      }
    }
  }

  // net uponEvent {
  //   case NetMessage(header, RouteMsg(key, msg)) => {
  //     val nodes = lut.get.lookup(key);
  //     assert(!nodes.isEmpty);
  //     val i = Random.nextInt(nodes.size);
  //     val target = nodes.drop(i).head;
  //     log.info(s"Forwarding message for key $key to $target");
  //     trigger(NetMessage(header.src, target, msg) -> net);
  //   }
  //   case NetMessage(header, msg: Connect) => {
  //     lut match {
  //       case Some(l) => {
  //         log.debug("Accepting connection request from ${header.src}");
  //         val size = l.getNodes().size;
  //         trigger(NetMessage(self, header.src, msg.ack(size)) -> net);
  //       }
  //       case None => log.info("Rejecting connection request from ${header.src}, as system is not ready, yet.");
  //     }
  //   }
  // }
  net uponEvent {
    case NetMessage(header, RouteMsg(killCmd, dm: Debug)) if killCmd startsWith "Kill/key:" => {
      val key = killCmd.replace("Kill/key:", "")
      val partIterator = lut.get.lookup(key).iterator
      var addr: Option[NetAddress] = None
      do {
        addr = Some(partIterator.next())
      } while (addr.contains(self))
      trigger(NetMessage(header.src, addr.get, Debug("Suicide", dm.source)) -> net)
    }
    case NetMessage(header, RouteMsg("FailureDetect", dm: Debug)) => {
      trigger(NetMessage(self, header.src, dm.response(OpCode.Ok, Some(lut.get.getPartitionsAsString()))) -> net)
      trigger(BEB_Broadcast_Global(dm) -> beb)
    }
    case NetMessage(_, RouteMsg("BroadcastFlood", dm: Debug)) => {
      trigger(BEB_Broadcast_Global(dm) -> beb)
    }
    case NetMessage(header, RouteMsg("ExtractPartitionInfo", _: Debug)) =>  {
      // do not use broadcast to test partitions
      for (tuple <- lut.get.partitions; address <- tuple._2) {
        val routeMsg = RouteMsg("PartitionInfo", Debug("PartitionInfo", self))
        trigger(NetMessage(header.src, address, routeMsg) -> net)
      }
    }
    case NetMessage(header, RouteMsg("PartitionInfo", dm: Debug)) => {
      trigger(NetMessage(self, header.src, dm.response(OpCode.Ok, Some(lut.get.getPartitionsAsString()))) -> net)
    }

    case NetMessage(header, RouteMsg(key, msg: Op)) => {
      val nodes = lut.get.lookup(key).toSet
      trigger(SetTopology(lut, nodes) -> beb);
      trigger(BEB_Broadcast(msg) -> beb);
    }

    case NetMessage(header, msg: Connect) => {
      println("Received connection request from client")
      lut match {
        case Some(l) =>
          println(s"Accepting connection request from ${header.src}")
          val size = l.getNodes().size
          trigger(NetMessage(self, header.src, msg.ack(size)) -> net)
        case None => log.info(s"Rejecting connection request from ${header.src}, as system is not ready, yet.");
      }
    }
  }



  route uponEvent {
    case RouteMsg(key, msg) => {
      val nodes = lut.get.lookup(key).toSet;
      // assert(!nodes.isEmpty);
      // val i = Random.nextInt(nodes.size);
      // val target = nodes.drop(i).head;
      // log.info(s"Routing message for key $key to $target");
      // trigger(NetMessage(self, target, msg) -> net);
      trigger(SetTopology(lut, nodes) -> beb);
      trigger(BEB_Broadcast(msg) -> beb);
    }
  }
}
