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

import schema._
import org.scalatest.junit.JUnitSuite
import org.junit.Test
import org.junit.Assert._
import java.util.UUID

class UsageTest extends JUnitSuite {
  @Test def testDsl() {
    import schema.Conversions._

    // Standard Column Family operations
    val stdKey =         "StandardFamily"|"key"
    val stdColumnName =  "StandardFamily"|"key"|"column"
    val stdColumnValue = "StandardFamily"|"key"|"column".as[String]
    val stdColumn =      "StandardFamily"|"key"|("column", "value")
    val stdKeyRange =    "StandardFamily"\\("start","end",20)
    val stdSliceRange =  "StandardFamily"|"key"\\("start","finish")
    val stdPredicate =   "StandardFamily"|"key"\Array("column","column3","column3")
    "StandardFamily"|"key"|"column" <= "New Vaue"

    // Counter column family operations
    val countKey =         "CountFamily"|"key"
    val countColumnName =  "CountFamily"|"key"|"column"
    val countColumnValue = "CountFamily"|"key"|"column".as[String]
    val countColumn =      "CountFamily"|"key"|("column", "value")
    val countKeyRange =    "CountFamily"\\("start","end",20)
    val countSliceRange =  "CountFamily"|"key"\\("start","finish")
    val countPredicate =   "CountFamily"|"key"\Array("column","column3","column3")
    "CounterFamily"|"key"|"column" += 5
    "CounterFamily"|"key"|"column" -= 2


    // SuperColumn family operations
    val superKey =         "SuperFamily"|"key"
    val superColumnSCol =  "SuperFamily"|"key"|"superColumn"
    val superColumnName =  "SuperFamily"|"key"|"superColumn"|"column".as[String]
    val superColumn =      "SuperFamily"|"key"|"superColumn"|("column", "value")
    val superColumnPred =  "SuperFamily"|"key"\Array("column", "column2")
    val superColumnPred2=  "SuperFamily"|"key"|"superColumn"\Array("column", "column2")
    val superColumnSlice = "SuperFamily"|"key"\\("start", "end")
    val superColumnSlice2= "SuperFamily"|"key"|"superColumn"\\("start", "end")
    "SuperFamily"|"key"|"superColumn"|"column" <= "New Vaue"
  }
}