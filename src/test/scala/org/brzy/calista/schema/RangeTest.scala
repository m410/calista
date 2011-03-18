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
import org.brzy.calista.results.{SuperColumn=>RSuperColumn}
import org.brzy.calista.serializer.UTF8Serializer


class RangeTest extends JUnitSuite with EmbeddedTest {

  @Test def testPredicateOnStandard = {
    import Conversions._

    sessionManager.doWith { session =>
      session.insert("Standard"|"key-range-0"|("column", "value0"))
      session.insert("Standard"|"key-range-1"|("column", "value1"))
      session.insert("Standard"|"key-range-2"|("column", "value2"))
      session.insert("Standard"|"key-range-3"|("column", "value3"))
      session.insert("Standard"|"key-range-4"|("column", "value4"))
    }

    sessionManager.doWith { session =>
      val standardKeyRange:KeyRange[String,String] = "Standard" \("key-range-4","key-range",List("column"))
      val keys = session.keyRange(standardKeyRange)
      keys.keySet.foreach(k=>println("##k='%s'".format(k)))
      println(keys)
      assertNotNull(keys)
      assertEquals(1,keys.size)
    }
  }

  @Test def testSuperSlice = {
    import Conversions._
    
    sessionManager.doWith { session =>
      session.insert("Super2"|^"key"|"super-col-0"|("column", "value0"))
      session.insert("Super2"|^"key"|"super-col-0"|("column1", "value0"))
      session.insert("Super2"|^"key"|"super-col-1"|("column", "value1"))
      session.insert("Super2"|^"key"|"super-col-1"|("column1", "value1"))
      session.insert("Super2"|^"key"|"super-col-2"|("column", "value2"))
      session.insert("Super2"|^"key"|"super-col-2"|("column1", "value2"))
      session.insert("Super2"|^"key"|"super-col-3"|("column", "value3"))
      session.insert("Super2"|^"key"|"super-col-3"|("column1", "value3"))
      session.insert("Super2"|^"key2"|"super-col-4"|("column", "value4"))
      session.insert("Super2"|^"key2"|"super-col-4"|("column1", "value4"))
    }

    sessionManager.doWith { session =>
      val slice = {"Super2" |^ "key"} \("super-col-0","super-col-3")
      val rows = session.sliceRange(slice)
      rows.foreach(k=>{
        val sCol = k.asInstanceOf[RSuperColumn[_]]
        val fromBytes = UTF8Serializer.fromBytes(sCol.bytes)
        println("######")
        println(fromBytes)
        println("######")
        sCol.columns.foreach(c=>{
          val name = c.nameAs(UTF8Serializer)
          val value = c.valueAs(UTF8Serializer)
          println("column: "+name+"="+value)
        })
      })
      println(rows)
      assertNotNull(rows)
      assertEquals(4,rows.size)

    }
  }
}