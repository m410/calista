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
import org.brzy.calista.dsl.Conversions


class RemoveTest extends JUnitSuite with EmbeddedTest {
  @Test def removeTest() {
    import Conversions._

    sessionManager.doWith { session =>
      val key = "Standard" | "remover"
        session.insert(key | ("column5", "value0"))

      val key2 = "Standard" | "remover2"
      session.insert(key2 | ("column5", "value0"))
    }

    sessionManager.doWith { session =>
      val key = "Standard" | "remover"
      val columnName = key | "column5"
      session.remove(columnName)
    }

    sessionManager.doWith { session =>
      val key = "Standard" | "remover2"
      val columnName = key | "column5"
      assert(columnName.remove)
    }

    sessionManager.doWith { session =>
      val key = "Standard" | "remover"
      val stdKey = key | "column5"
      val result = session.list(stdKey)
      assertNotNull(result)
      assertTrue(result.isEmpty)


      val key2 = "Standard" | "remover2"
      val stdKey2 = key2 | "column5"
      val result2 = session.list(stdKey2)
      assertNotNull(result2)
      assertTrue(result2.isEmpty)
    }
  }
}