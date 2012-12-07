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


class RemoveTest extends JUnitSuite with EmbeddedTest {
  @Test def removeTest() {

    sessionManager.doWith { session =>
      StandardColumnFamily("Standard")("remover")("column5").set("value0")
      StandardColumnFamily("Standard")( "remover2")("column5").set("value0")
    }

    sessionManager.doWith { session =>
      StandardColumnFamily("Standard")("remover")("column5").remove()
    }

    sessionManager.doWith { session =>
      StandardColumnFamily("Standard")( "remover2")("column5").remove()
    }

    sessionManager.doWith { session =>
      val opt = StandardColumnFamily("Standard")("remover")("column5").getAs[String]
      assertTrue(opt.isEmpty)


      val opt2 = StandardColumnFamily("Standard")( "remover2")("column5").getAs[String]
      assertTrue(opt2.isEmpty)
    }
  }
}