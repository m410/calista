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
package org.brzy.calista.schema

import org.scalatest.junit.JUnitSuite
import org.junit.Test
import org.junit.Assert._
import org.brzy.calista.server.EmbeddedTest


class CountTest extends JUnitSuite with EmbeddedTest {
  @Test def countTest() {
    sessionManager.doWith { session =>
      val key = StandardFamily("Standard")("count")
      key("column5").set("value0")
      key("column4").set("value1")
      key("column3").set("value2")
      key("column2").set("value3")
      key("column1").set("value4")
    }

    sessionManager.doWith { session =>
      val key = StandardFamily("Standard")("count")
      val amount = session.count(key)
      assertNotNull(amount)
      assertEquals(5,amount)
    }
  }
}