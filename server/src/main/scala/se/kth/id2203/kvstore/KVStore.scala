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
package se.kth.id2203.kvstore;

import java.util.UUID

import se.kth.id2203.networking._
import se.kth.id2203.overlay.Routing
import se.sics.kompics.sl._
import se.sics.kompics.network.Network

import scala.collection.mutable
import scala.concurrent.Promise;

class KVService extends ComponentDefinition {

  //******* Ports ******
  val net = requires[Network];
  val route = requires(Routing);
  val consensus = requires[Consensus]
  //******* Fields ******
  val self = cfg.getValue[NetAddress]("id2203.project.address");
  private val keyValueMap = mutable.SortedMap.empty[Int, Any];
  private val pending = mutable.SortedMap.empty[UUID, NetAddress];
  //******* Handlers ******
  net uponEvent {
    case NetMessage(NetHeader(src, _, _), get: GetOp) => {
      log.info("Got GET operation {}! Header: {};", get, header);
      trigger(NetMessage(self, header.src, OpResponse(get.id, OpCode.NotImplemented, 0)) -> net);
    }

    case NetMessage(NetHeader(src, _, _), put: PutOp) => {
      log.info("Got PUT operation {}! Header: {};", put, header);
      trigger(Propose(put.key, (put.id, put.value)) -> consensus);
      pending += ((put.id, src));
    }

    case NetMessage(NetHeader(src, _, _), cas: CasOp) => {
      log.info("Got CAS operation {}! Header: {};", cas, header);
      trigger(NetMessage(self, header.src, OpResponse(cas.id, OpCode.NotImplemented, 0)) -> net);
    }
  }

  consensus uponEvent {
    case Decide(key: Int, value: (UUID, Any)) => {
      keyValueMap(key) = value._2;
      if (pending.contains(value._1)) {
        trigger(NetMessage(self, pending(value._1), OpResponse(value._1, OpCode.Ok, value)) -> net);
      }
    }
  }
}
