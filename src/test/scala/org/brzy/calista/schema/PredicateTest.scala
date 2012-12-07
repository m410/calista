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


class PredicateTest extends JUnitSuite with EmbeddedTest  {

  @Test def testPredicateOnStandard() {

    sessionManager.doWith { session =>
      val key = StandardFamily("Standard")("predicate-key")
      session.insert(key.column("column0", "value0"))
      session.insert(key.column("column1", "value1"))
      session.insert(key.column("column2", "value2"))
      session.insert(key.column("column3", "value3"))
      session.insert(key.column("column4", "value4"))
    }

    sessionManager.doWith { session =>
      val key = StandardFamily("Standard")("predicate-key")
      val standardSlicePredicate = key.predicate(Array("column1","column2","column3"))
      val columns = session.slice(standardSlicePredicate)
      assertNotNull(columns)
      assertEquals(3,columns.size)
    }
  }
}