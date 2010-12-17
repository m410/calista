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
package org.brzy.calista

import column.KeyRange
import schema.Utf8Type
import server.EmbeddedTest
import org.scalatest.junit.JUnitSuite
import org.junit.Test
import org.junit.Assert._


class RangeTest extends JUnitSuite with EmbeddedTest {

  @Test def testPredicateOnStandard = {
    val sessionManager = new SessionManager()
    import column.Conversions._

    sessionManager.doWith { session =>
      session.insert("Standard"|"key-range-0" | ("column", "value0"))
      session.insert("Standard"|"key-range-1" | ("column", "value1"))
      session.insert("Standard"|"key-range-2" | ("column", "value2"))
      session.insert("Standard"|"key-range-3" | ("column", "value3"))
      session.insert("Standard"|"key-range-4" | ("column", "value4"))
    }

    sessionManager.doWith { session =>
      val standardKeyRange:KeyRange[String,String] = "Standard" \("key-range-4","key-range",List("column"))
      val keys = session.get(standardKeyRange)
      keys.keySet.foreach(k=>println("##k='%s'".format(k)))
      println(keys)
      assertNotNull(keys)
      assertEquals(1,keys.size)
    }
  }
}