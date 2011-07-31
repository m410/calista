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
import org.brzy.calista.serializer.UTF8Serializer
import org.brzy.calista.dsl.Conversions
import org.brzy.calista.results.ResultSet


class RangeTest extends JUnitSuite with EmbeddedTest {

  // key ranges are not implemented anymore
  @Test def testPredicateOnStandard() {
    import Conversions._

    sessionManager.doWith { session =>
      session.insert("Standard"|"key-range"|("column1", "value0"))
      session.insert("Standard"|"key-range"|("column2", "value1"))
      session.insert("Standard"|"key-range"|("column3", "value2"))
      session.insert("Standard"|"key-range"|("column4", "value3"))
      session.insert("Standard"|"key-range"|("column5", "value4"))
    }

    sessionManager.doWith { session =>
      val range = {"Standard" | "key-range"}\\("column2","column4",true)
      val result = range.results
      result.rows.foreach(k=>println("##column='%s'".format(k)))
      assertNotNull(result)
      assertEquals(3,result.size)
    }
  }

  @Test def testSuperSlice() {
    import Conversions._
    
    sessionManager.doWith { session =>
      session.insert("Super2"||"key"|"super-col-0"|("column", "value0"))
      session.insert("Super2"||"key"|"super-col-0"|("column1", "value0"))
      session.insert("Super2"||"key"|"super-col-1"|("column", "value1"))
      session.insert("Super2"||"key"|"super-col-1"|("column1", "value1"))
      session.insert("Super2"||"key"|"super-col-2"|("column", "value2"))
      session.insert("Super2"||"key"|"super-col-2"|("column1", "value2"))
      session.insert("Super2"||"key"|"super-col-3"|("column", "value3"))
      session.insert("Super2"||"key"|"super-col-3"|("column1", "value3"))
      session.insert("Super2"||"key2"|"super-col-4"|("column", "value4"))
      session.insert("Super2"||"key2"|"super-col-4"|("column1", "value4"))
    }

    sessionManager.doWith { session =>
      val slice = {"Super2"||"key"}\\("super-col-0","super-col-3")
      val rows = session.sliceRange(slice.asInstanceOf[SliceRange[String]])
      assertNotNull(rows)
      assertEquals(8,rows.size)

    }
  }
}