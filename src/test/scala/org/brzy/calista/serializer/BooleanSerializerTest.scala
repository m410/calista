/*
 * Copyright 2010 Michael Fortin <mike@brzy.org>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");  you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed 
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR 
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package org.brzy.calista.serializer

import org.scalatest.junit.JUnitSuite
import org.junit.Test
import org.junit.Assert._


class BooleanSerializerTest extends JUnitSuite {
  @Test def testBooleanUnapply() {
    val t = true
    t match {
      case BooleanSerializer(x) =>
        val b = x.asInstanceOf[Boolean]
        assertTrue(b)
      case _ => fail()
    }

    val f = false
    f match {
      case BooleanSerializer(x) =>
        val b = x.asInstanceOf[Boolean]
        assertTrue(!b)
      case _ => fail()
    }

  }

  @Test def testBooleanType() {
    val t = false
    def buf = BooleanSerializer.toBytes(t)
    assertEquals(1, buf.array.length)
  }

  @Test def testTypes() {
    val t = true
    def buf = Serializers.toBytes(t)
    assertEquals(1, buf.array.length)
    val bool = buf.array()(0) == 1.asInstanceOf[Byte]
    assertTrue(bool)
  }
}