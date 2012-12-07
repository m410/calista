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
import java.util.Date


class RangeTest extends JUnitSuite with EmbeddedTest {

  // key ranges are not implemented anymore
  @Test def testPredicateOnStandard() {

    sessionManager.doWith { session =>
      StandardColumnFamily("Standard")("key-range")("column1").set("value0")
      StandardColumnFamily("Standard")("key-range")("column2").set("value1")
      StandardColumnFamily("Standard")("key-range")("column3").set("value2")
      StandardColumnFamily("Standard")("key-range")("column4").set("value3")
      StandardColumnFamily("Standard")("key-range")("column5").set("value4")
    }

    sessionManager.doWith { session =>
      val predicate = StandardColumnFamily("Standard")("key-range").predicate(Array("column4","column2"))
      val result = predicate.results
      assertNotNull(result)
      assertEquals(3,result.size)
    }
  }

  @Test def testSliceRangeScroll() {
    sessionManager.doWith { session =>
      for (i <- 10 to  100) {
        val columnName = "column-"+i
        val valueName = "value-"+i
        val c = StandardColumnFamily("Standard")("key-range1").column(columnName, valueName)
        session.insert(c)
      }
    }
    sessionManager.doWith { session =>
      val range = StandardColumnFamily("Standard")("key-range1").from("").reverse.size(32)
      var count = 0
      val iterator = range.iterator
      
      while(iterator.hasNext) {
        val nextRow = iterator.next()
        count = count + 1
      }

      assertEquals(91,count)
    }
  }

  @Test def testSliceRangeScrollWithLong() {
    sessionManager.doWith { session =>
      for (i <- 10 to  100) {
        val columnName = i.longValue()
        val valueName = "value-"+i
        val c = StandardColumnFamily("StandardInt")("1").column(columnName, valueName)
        session.insert(c)
      }
    }
    sessionManager.doWith { session =>
      val range = StandardColumnFamily("StandardInt")("1").from(0).to(100).size(32)
      var count = 0
      val iterator = range.iterator

      while(iterator.hasNext) {
        iterator.next()
        count = count + 1
      }

      assertEquals(91,count)
    }
  }

  @Test def testSliceRangeScrollWithTwoLongs() {
    sessionManager.doWith { session =>
      for (i <- 10 to  100) {
        val columnName = i.longValue()
        val valueName = "value-"+i
        val c = StandardColumnFamily("StandardLong")(1L).column(columnName, valueName)
        session.insert(c)
      }
    }
    sessionManager.doWith { session =>
      val range = StandardColumnFamily("StandardLong")(1L).from(0).size(32)
      var count = 0
      val iterator = range.iterator

      while(iterator.hasNext) {
        val nextRow = iterator.next()
        count = count + 1
      }

      assertEquals(91,count)
    }
  }

  @Test def testSuperSlice() {

    sessionManager.doWith { session =>
      SuperColumnFamily("Super2")("skey1")("scol0")("column0").set("value0")
      SuperColumnFamily("Super2")("skey1")("scol0")("column1").set("value0")
      SuperColumnFamily("Super2")("skey1")("scol1")("column0").set("value1")
      SuperColumnFamily("Super2")("skey1")("scol1")("column1").set("value1")

      SuperColumnFamily("Super2")("skey1")("scol2")("column0").set("value2")
      SuperColumnFamily("Super2")("skey1")("scol2")("column1").set("value2")
      SuperColumnFamily("Super2")("skey1")("scol3")("column0").set("value3")
      SuperColumnFamily("Super2")("skey1")("scol3")("column1").set("value3")
      SuperColumnFamily("Super2")("skey2")("scol4")("column0").set("value4")
      SuperColumnFamily("Super2")("skey2")("scol4")("column1").set("value4")
    }

    sessionManager.doWith { session =>
      SuperColumnFamily("Super2")("skey1")("scol0")("none").getAs[String] match {
        case Some(v) => fail("Should have returned nothing: " + v)
        case _ =>
      }

      SuperColumnFamily("Super2")("skey1")("scol0")("column0").getAs[String] match {
        case Some(v) =>
          assertEquals("value0",v)
        case _ => fail("No value returned")
      }
    }
    sessionManager.doWith { session =>
      val slice = SuperColumnFamily("Super2")("skey1")("scol0").from("a").to("z")
      val results = session.sliceRange(slice)
      assertNotNull(results)
      assertEquals(2,results.size)
    }

    sessionManager.doWith { session =>
      val slice = SuperColumnFamily("Super2")("skey1").from("a").to("z")
      val results = session.sliceRange(slice)
      assertNotNull(results)
      results.rows.foreach(r=>{
        assertEquals("skey1",r.keyAs[String])
      })
      assertEquals(8,results.size)
    }
  }
}