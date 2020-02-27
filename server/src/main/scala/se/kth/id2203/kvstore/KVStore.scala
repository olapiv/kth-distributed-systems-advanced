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
package se.kth.id2203.kvstore

;

import se.kth.id2203.networking._
import se.kth.id2203.overlay.Routing
import se.sics.kompics.network.Network
import se.sics.kompics.sl._

import scala.collection.mutable;

class KVService extends ComponentDefinition {

  //******* Ports ******
  val net: PositivePort[Network] = requires[Network];
  val route: PositivePort[Routing.type] = requires(Routing);
  val sc: PositivePort[SequenceConsensus] = requires[SequenceConsensus];

  //******* Fields ******
  val self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address");

  // members
  val keyValueMap = mutable.Map.empty[String, String]

  for (i <- 0 to 10) {
    keyValueMap += ((i.toString, (100 + i).toString))
  }

  //******* Handlers ******
  net uponEvent {
    case NetMessage(_, op: Op) => {
      trigger(SC_Propose(op) -> sc);
    }
  }

  sc uponEvent {
    case SC_Decide(op: Get) => {
      println(s"GET operation $op");
      val result = if (keyValueMap contains op.key) Some(keyValueMap(op.key)) else None
      trigger(NetMessage(self, op.source, op.response(OpCode.Ok, result)) -> net);
    }
    case SC_Decide(op: Put) => {
      println(s"PUT operation $op");
      keyValueMap += ((op.key, op.value))
      trigger(NetMessage(self, op.source, op.response(OpCode.Ok, Some(op.value))) -> net);
    }
    case SC_Decide(op: Cas) => {
      println(s"CAS operation $op");
      val storedValue: Option[String] = if (keyValueMap.get(op.key).isDefined) Some(keyValueMap(op.key)) else None
      if (storedValue.isDefined && storedValue.get == op.oldValue) keyValueMap += ((op.key, op.newValue))
      trigger(NetMessage(self, op.source, op.response(OpCode.Ok, storedValue)) -> net)
    }
  }
}
