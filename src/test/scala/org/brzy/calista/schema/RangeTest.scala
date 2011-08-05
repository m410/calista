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
//      result.rows.foreach(k=>println("##column='%s'".format(k)))
      assertNotNull(result)
      assertEquals(3,result.size)
    }
  }

  @Test def testSuperSlice() {
    import Conversions._
    
    sessionManager.doWith { session =>
      {"Super2"||"skey1"|"scol0"|"column0"} << "value0";
      {"Super2"||"skey1"|"scol0"|"column1"} << "value0";
      {"Super2"||"skey1"|"scol1"|"column0"} << "value1";
      {"Super2"||"skey1"|"scol1"|"column1"} << "value1";
      session.insert("Super2"||"skey1"|"scol2"|("column0", "value2"))
      session.insert("Super2"||"skey1"|"scol2"|("column1", "value2"))
      session.insert("Super2"||"skey1"|"scol3"|("column0", "value3"))
      session.insert("Super2"||"skey1"|"scol3"|("column1", "value3"))
      session.insert("Super2"||"skey2"|"scol4"|("column0", "value4"))
      session.insert("Super2"||"skey2"|"scol4"|("column1", "value4"))
    }

    sessionManager.doWith { session =>
      {"Super2"||"skey1"|"scol0"|"none"}.valueAs[String] match {
        case Some(v) => fail("Should have returned nothing: " + v)
        case _ =>
      }

      {"Super2"||"skey1"|"scol0"|"column0"}.valueAs[String] match {
        case Some(v) =>
          assertEquals("value0",v)
        case _ => fail("No value returned")
      }
    }
    sessionManager.doWith { session =>
      val slice = {"Super2"||"skey1"|"scol0"}\\("a","z")
      val results = session.sliceRange(slice)
      assertNotNull(results)
//      results.rows.foreach(r=>{
//        println("####### " + r.keyAs[String] +"|"+ r.columnAs[String]+"|"+ r.valueAs[String])
//      })
      assertEquals(2,results.size)
    }

    sessionManager.doWith { session =>
      val slice = {"Super2"||"skey1"}\\("a","z")
      val results = session.sliceRange(slice)
      assertNotNull(results)
      results.rows.foreach(r=>{
        println("#### " + r.keyAs[String] +"|"+ r.superColumnAs[String]+"|"+ r.columnAs[String]+"|"+ r.valueAs[String])
        assertEquals("skey1",r.keyAs[String])
      })
      assertEquals(8,results.size)
    }
  }
}