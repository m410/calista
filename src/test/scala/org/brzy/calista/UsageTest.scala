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
import results.{Row, ResultSet}
import schema._
import server.EmbeddedTest


class UsageTest extends JUnitSuite with EmbeddedTest {

  @Test def testKeyRange() {
    sessionManager.doWith {
      session =>
        StandardFamily("StandardFamily")("key1")("column").set("v")
        StandardFamily("StandardFamily")("key2")("column").set("v")
        StandardFamily("StandardFamily")("key3")("column").set("v")
    }
    sessionManager.doWith {
      session =>
        val keyRange = StandardFamily("StandardFamily").from("key").predicate(Array("column")).size(10)
        assertNotNull(keyRange)
        val results = keyRange.list
        assertNotNull(results)
        assertEquals(3, results.size)
    }
  }

  @Test def testColumnFamilyStandardCol() {
    sessionManager.doWith {
      session =>
        println("###########  " + session.ksDef)

        val stdColName = StandardFamily("StandardFamily")("key")("column")
        assertNotNull(stdColName)
        assertTrue(stdColName.isInstanceOf[ColumnName[_]])

        StandardFamily("StandardFamily")("key")("column").set("somevalue")
        val values = StandardFamily("StandardFamily")("key").map(_.valueAs[String])
        assert(values != null)
        assert(values.size == 1)

        StandardFamily("StandardFamily")("key").foreach(r => println(r.valueAs[String]))
    }
  }

  @Test def testStandardCol() {
    sessionManager.doWith {
      session =>
        val key = StandardFamily("StandardFamily")("key")
        assertTrue(key.isInstanceOf[StandardKey[_]])
        val stdColumnName = StandardFamily("StandardFamily")("key")("column")
        assertTrue(stdColumnName.isInstanceOf[ColumnName[_]])

        key("column").set("New Value")
        val stdColumnValue = key("column").getAs[String]
        assertTrue(stdColumnValue.isDefined)
        assertEquals("New Value", stdColumnValue.get)

        val col = key.column("name", "value")
        assertTrue(col.isInstanceOf[Column[_, _]])

        val stdSliceRange = key.from("begin").to("finish").reverse(true)
        assertTrue(stdSliceRange.isInstanceOf[SliceRange])

        val stdSliceRange2 = key.from("begin").to("end").limit(10).iterator
        assertTrue(stdSliceRange2.isInstanceOf[collection.Iterator[_]])

        val stdSliceRange3 = key.from("begin").to("end").limit(10).results
        assertTrue(stdSliceRange3.isInstanceOf[Seq[Row]])

        val stdPredicate = key.predicate(Array("column", "column2", "column3"))
        assertTrue(stdPredicate.isInstanceOf[SlicePredicate[_]])
    }
  }

  @Test def testSuperCol() {
    sessionManager.doWith {
      session =>
        val key = SuperFamily("SuperFamily")("key")
        assertTrue(key.isInstanceOf[SuperKey[_]])
        val superColumnSCol = SuperFamily("SuperFamily")("key")("superColumn")
        assertTrue(superColumnSCol.isInstanceOf[SuperColumn[_]])

        key("superColumn")("column").set("New Value")
        val superColumnName = key("superColumn")("column").getAs[String]
        assertTrue(superColumnName.isDefined)
        assertEquals("New Value", superColumnName.get)

        val superColumnPred = key.predicate(Array("column", "column2"))
        assertTrue(superColumnPred.isInstanceOf[SlicePredicate[_]])

        val superColumnSlice = key.from("begin").to("end")
        assertTrue(superColumnSlice.isInstanceOf[SliceRange])

        val superColumnPred2 = key("superColumn").predicate(Array("column", "column2"))
        assertTrue(superColumnPred2.isInstanceOf[SlicePredicate[_]])

        val superColumnSlice2 = key("superColumn").from("begin").to("end")
        assertTrue(superColumnSlice2.isInstanceOf[SliceRange])
    }
  }

  @Test def testStandardCounterCol() {
    sessionManager.doWith {
      session =>
        val key = CounterFamily("CountFamily")("key")
        assertTrue(key.isInstanceOf[CounterKey[_]])

        val countColumnName = CounterFamily("CountFamily")("key")("column")
        assertTrue(countColumnName.isInstanceOf[CounterColumnName[_]])
        key("column1").add(10)

        val counter = key("column")
        assertTrue(counter.isInstanceOf[CounterColumnName[_]])
        counter.add(5)
        counter.add(3)
        counter += 2
        assert(counter.count == 10, "could should be 6 but was " + counter.count)

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
        SuperCounterFamily("SuperCountFamily")("key")("super")("column1").add(0L)
        SuperCounterFamily("SuperCountFamily")("key")("super")("column").add(0L)
    }

    sessionManager.doWith {
      session =>
        val scol = SuperCounterFamily("SuperCountFamily")("key")("super")
        val counter1 = scol("column1").count
        assert(counter1 == 0)
        scol("column1").add(10)

        assert(scol("column").count == 0)
        scol("column").add(5)
        scol("column").add(3)
        scol("column").add(-2)
        assert(scol("column").count == 6)

        val range = scol.from("a").to("z")
        assertTrue(range.isInstanceOf[SliceRange])
        val results = range.results
        assertEquals(2, results.size)

        val predicate = scol.predicate(Array("column", "column1"))
        assertTrue(predicate.isInstanceOf[SlicePredicate[_]])
        val results2 = predicate.results
        assertEquals(2, results2.size)
    }
  }
}