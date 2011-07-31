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
  @Test def testDsl() {
    import dsl.Conversions._
    sessionManager.doWith({  session =>
      // Standard Column Family operations
        val stdKey = "StandardFamily" | "key"
        assertTrue(stdKey.isInstanceOf[StandardKey[_]])
        val stdColumnName = "StandardFamily" | "key" | "column"
        assertTrue(stdColumnName.isInstanceOf[ColumnName[_]])

        // get and set
        {"StandardFamily" | "key" | "column"} << "New Value"
        val stdColumnValue = {"StandardFamily" | "key" | "column"}.valueAs[String]
        assertTrue(stdColumnValue.isDefined)
        assertEquals("New Value",stdColumnValue.get)

        val col = "StandardFamily" | "key" | ("name", "value")
        assertTrue(col.isInstanceOf[Column[_,_]])
        val stdSliceRange = {"StandardFamily" | "key" }\\ ("begin", "finish")
        assertTrue(stdSliceRange.isInstanceOf[SliceRange[_]])
        val stdSliceRange2 = { {"StandardFamily" | "key" }\\ ("begin", "end", false, 10)}.iterate
        assertTrue(stdSliceRange2.isInstanceOf[java.util.Iterator[Row]])
        val stdSliceRange3 = { {"StandardFamily" | "key"} \\ ("begin", "end", false, 10)}.results
        assertTrue(stdSliceRange3.isInstanceOf[ResultSet])

        val stdPredicate = {"StandardFamily" | "key"} \ ("column", "column3", "column3")
        assertTrue(stdPredicate.isInstanceOf[SlicePredicate[_]])

        // Counter column family operations
        val countKey = "CountFamily" | "key"
        assertTrue(countKey.isInstanceOf[StandardKey[_]])
        val countColumnName = "CountFamily" | "key" | "column"
        assertTrue(countColumnName.isInstanceOf[ColumnName[_]])

        // get and add/subtract
        val counter = "CountFamily" | "key" |# "column"
        assertTrue(counter.isInstanceOf[CounterColumnName[String]])
        counter += 5
        counter += 3
        counter -= 2
        assert(counter.count == 6)

        val countSliceRange2 = {"CountFamily" | "key"} \\ ("begin", "end")
        assertTrue(countSliceRange2.isInstanceOf[SliceRange[_]])
        val countPredicate = {"CountFamily" | "key"} \ ("column", "column3", "column3")
        assertTrue(countPredicate.isInstanceOf[SlicePredicate[_]])


        // SuperColumn family operations
        val superKey = "SuperFamily" || "key"
        assertTrue(superKey.isInstanceOf[SuperKey[_]])
        val superColumnSCol = "SuperFamily" || "key" | "superColumn"
        assertTrue(superColumnSCol.isInstanceOf[SuperColumn[_]])

        // get and set
        {"SuperFamily" || "key" | "superColumn" | "column"} << "New Value"
        val superColumnName = {"SuperFamily" || "key" | "superColumn" | "column"}.valueAs[String]
        assertTrue(superColumnName.isDefined)
        assertEquals("New Value", superColumnName.get )

        val superColumnPred = {"SuperFamily" || "key"} \ ("column", "column2")
        assertTrue(superColumnPred.isInstanceOf[SlicePredicate[_]])
        val superColumnSlice = {"SuperFamily" || "key"} \\ ("begin", "end")
        assertTrue(superColumnSlice.isInstanceOf[SliceRange[_]])

        val superColumnPred2 = {"SuperFamily" || "key" | "superColumn"} \ ("column", "column2")
        assertTrue(superColumnPred2.isInstanceOf[SlicePredicate[_]])
        val superColumnSlice2 = {"SuperFamily" || "key" | "superColumn"} \\ ("begin", "end")
        assertTrue(superColumnSlice2.isInstanceOf[SliceRange[_]])
    })
  }
}