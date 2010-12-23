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


class LongSerializerTest extends JUnitSuite {
  @Test def testLongType = {
    def buf = LongSerializer.toBytes(500L)
    assertEquals(8,buf.array.length)
  }

  @Test def testTypes = {
    def buf = Serializers.toBytes(500L)
    assertEquals(8,buf.array.length)
    val lng = buf.getLong
    assertEquals(500,lng)
  }
}