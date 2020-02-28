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
  val route = provides(Routing)
  val boot= requires(Bootstrapping)
  val net = requires[Network]
  val timer = requires[Timer]
  val epfd = requires[EventuallyPerfectFailureDetector]
  val beb = requires[BestEffortBroadcast]

  //******* Fields ******
  val self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address")
  var seqCons = requires[SequenceConsensus]
  var suspected = Set[NetAddress]()
  var lut: Option[LookupTable] = None

  //******* Handlers ******
  beb uponEvent {
    case BEB_Deliver(src, payload) => {
      trigger(NetMessage(src, self, payload) -> net);
    }
  }

  epfd uponEvent {
    case Suspect(p) => {
      suspected += p
    }
    case Restore(p) => {
      suspected -= p
    }
  }

  boot uponEvent {
    case GetInitialAssignments(nodes) => {  // Request received from Bootstrap Server
      log.info("Generating LookupTable... nodes={}", nodes);
      val delta = cfg.getValue[Int]("id2203.project.delta")
      logger.debug(s"Delta: $delta")
      logger.debug(s"Nodes: $nodes")
      val lut = LookupTable.generate(nodes, delta);
      logger.debug(s"Generated assignments: $lut");
      trigger(new InitialAssignments(lut) -> boot);
    }
    case Booted(assignment: LookupTable) => {
      log.info("assignment: ", assignment);
      log.info("Got NodeAssignment, overlay ready.");
      lut = Some(assignment);
      val currentPartition = assignment.partitions.find(_._2.exists(_.equals(self)))
      currentPartition match {
        case Some((_, cp)) =>
          trigger(BEB_Topology(cp.toSet) -> beb)
          trigger(StartDetector(lut, cp.toSet) -> epfd)
          trigger(StartSequenceCons(cp.toSet) -> seqCons)
        case None =>
          print("Current partition not found")
      }
    }
  }

  net uponEvent {

    // Specific case for automated testing for killing a server node
    case NetMessage(header, RouteMsg(killCmd, dm: Get)) if killCmd startsWith "KILL:" => {
      val key = killCmd.replace("KILL:", "")
      val partIterator = lut.get.lookup(key).iterator
      var addr: Option[NetAddress] = None
      do {
        addr = Some(partIterator.next())
      } while (addr.contains(self))
      trigger(NetMessage(header.src, addr.get, Get("Suicide", dm.source)) -> net)
    }

    case NetMessage(_, RouteMsg(key, msg: Op)) => {
      val nodes = lut.get.lookup(key).toSet
      trigger(BEB_Topology(nodes) -> beb);
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
      trigger(BEB_Topology(nodes) -> beb);
      trigger(BEB_Broadcast(msg) -> beb);
    }
  }
}
