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

import org.scalatest.junit.JUnitSuite
import org.junit.Test
import org.junit.Assert._
import results.{ResultSet, Row}
import schema._
import server.EmbeddedTest


class UsageTest extends JUnitSuite with EmbeddedTest {

  @Test def testColumnFamilyStandardCol() {
    sessionManager.doWith { session =>
      val stdColName = StandardFamily("StandardFamily")("key")("column")
      assertNotNull(stdColName)
      assertTrue(stdColName.isInstanceOf[ColumnName])

      StandardFamily("StandardFamily")("key")("column").set("somevalue")
      val values = StandardFamily("StandardFamily")("key").map(_.valueAs[String])
      assert(values != null)
      assert(values.size == 1)

      StandardFamily("StandardFamily")("key").foreach(r=>println(r.valueAs[String]))
    }
  }

  @Test def testStandardCol() {
    sessionManager.doWith { session =>
        val key = StandardFamily("StandardFamily")("key")
        assertTrue(key.isInstanceOf[StandardKey])
        val stdColumnName = StandardFamily("StandardFamily")("key")("column")
        assertTrue(stdColumnName.isInstanceOf[ColumnName])

        key("column").set("New Value")
        val stdColumnValue = key("column").getAs[String]
        assertTrue(stdColumnValue.isDefined)
        assertEquals("New Value", stdColumnValue.get)

        val col = key.column("name", "value")
        assertTrue(col.isInstanceOf[Column])
        val stdSliceRange = key.from("begin").to("finish")
        assertTrue(stdSliceRange.isInstanceOf[SliceRange])
        val stdSliceRange2 = key.from("begin").to("end").reverse.size(10).iterator
        assertTrue(stdSliceRange2.isInstanceOf[collection.Iterator[Row]])
        val stdSliceRange3 = key.from("begin").to("end").reverse.size(10).results
        assertTrue(stdSliceRange3.isInstanceOf[ResultSet])

        val stdPredicate = key.predicate(Array("column", "column2", "column3"))
        assertTrue(stdPredicate.isInstanceOf[SlicePredicate[_]])
    }
  }

  @Test def testSuperCol() {
    sessionManager.doWith { session =>
        val key = SuperFamily("SuperFamily")("key")
        assertTrue(key.isInstanceOf[SuperKey])
        val superColumnSCol = SuperFamily("SuperFamily")("key")("superColumn")
        assertTrue(superColumnSCol.isInstanceOf[SuperColumn])

        key("superColumn")("column").set("New Value")
        val superColumnName = key( "superColumn")("column").getAs[String]
        assertTrue(superColumnName.isDefined)
        assertEquals("New Value", superColumnName.get)

        val superColumnPred = key("column")("column2")
        assertTrue(superColumnPred.isInstanceOf[SlicePredicate[_]])
        val superColumnSlice = key.from("begin").to("end")
        assertTrue(superColumnSlice.isInstanceOf[SliceRange])

        val superColumnPred2 = key("superColumn").column("column", "column2")
        assertTrue(superColumnPred2.isInstanceOf[SlicePredicate[_]])
        val superColumnSlice2 = key("superColumn").from("begin").to("end")
        assertTrue(superColumnSlice2.isInstanceOf[SliceRange])
    }
  }

  @Test def testStandardCounterCol() {
    sessionManager.doWith { session =>
        val key = CounterFamily("CountFamily")("key")
        assertTrue(key.isInstanceOf[CounterKey])
        val countColumnName = CounterFamily("CountFamily")("key")("column")
        assertTrue(countColumnName.isInstanceOf[CounterColumnName])

        key("column1").add(10)
        val counter = key("column")
        assertTrue(counter.isInstanceOf[CounterColumnName])
        counter.add(5)
        counter.add(3)
        counter.add(2)
        assert(counter.count == 6)

        val range = key.from("a").to("z")
        assertTrue(range.isInstanceOf[SliceRange])
        val results = range.results
        assertEquals(2, results.size)

        val predicate = key.predicate(Array("column", "column1"))
        assertTrue(predicate.isInstanceOf[SlicePredicate[_]])
        val results2 = predicate.results
        assertEquals(2, results2.size)
    }
  }

  @Test def testSuperCounterCol() {
    sessionManager.doWith {
      session =>
        val scol = SuperCounterFamily("SuperCountFamily")("key")("super")
        val counter1 = scol("column1").count
        scol("column1").add(10)
        scol("column").count
        scol("column").add(5)
        scol("column").add(3)
        scol("column").add(-2)
        assert(scol("column").count == 6)

        val range = scol.from("a").to("z")
        assertTrue (range.isInstanceOf[SliceRange])
        val results = range.results
        assertEquals(2, results.size)

        val predicate = scol.predicate(Array("column", "column1"))
        assertTrue(predicate.isInstanceOf[SlicePredicate[_]])
        val results2 = predicate.results
        assertEquals(2, results2.size)
    }
  }
}