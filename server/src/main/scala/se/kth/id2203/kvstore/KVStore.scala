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
  val consensus = requires[SequenceConsensus]
  //******* Fields ******
  val self = cfg.getValue[NetAddress]("id2203.project.address");
  private val keyValueMap = mutable.Map.empty[Int, Any];

  //******* Handlers ******
  net uponEvent {
    case NetMessage(NetHeader(src, _, _), get: GetOp) => {
      log.info("Got GET operation {}!", get);
      trigger(SC_Propose(get) -> consensus)
    }

    case NetMessage(NetHeader(src, _, _), put: PutOp) => {
      log.info("Got PUT operation {}!", put);
      trigger(SC_Propose(put) -> consensus)
    }

    case NetMessage(NetHeader(src, _, _), cas: CasOp) => {
      log.info("Got CAS operation {}!", cas);
      trigger(SC_Propose(cas) -> consensus)
    }
  }

  consensus uponEvent {
    case SC_Decide(get: GetOp) => {
      log.info("Done GET operation", get)
      if (keyValueMap.contains(get.key)) {
        trigger(NetMessage(self, get.src, OpResponse(get.id, OpCode.Ok, keyValueMap(get.key))) -> net)
      } else {
        trigger(NetMessage(self, get.src, OpResponse(get.id, OpCode.Ok, None)) -> net)
      }
    }
    case SC_Decide(put: PutOp) => {
      log.info("Done PUT operation", put)
      keyValueMap += ((put.key, put.value))
      trigger(NetMessage(self, put.src, OpResponse(put.id, OpCode.Ok, put.value)) -> net)
    }
    case SC_Decide(cas: CasOp) => {
      log.info("Done CAS operation", cas)
      if (keyValueMap.contains(cas.key)) {
        if (keyValueMap(cas.key).equals(cas.referenceValue)) {
          keyValueMap(cas.key) = cas.value
        }
      }
      trigger(NetMessage(self, cas.src, OpResponse(cas.id, OpCode.Ok, cas.value)) -> net)
    }
  }
}
