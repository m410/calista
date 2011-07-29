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

class UsageTest extends JUnitSuite {
  @Test def testDsl() {
    import dsl.Conversions._

    // Standard Column Family operations
    val stdKey =         "StandardFamily"|"key"
    val stdColumnName =  "StandardFamily"|"key"|"column"

    // get and set
    val stdColumnValue = "StandardFamily"|"key"|"column".valueAs[String]
    "StandardFamily"|"key"|"column" <= "New Vaue"

    val stdSliceRange =  "StandardFamily"|"key"\\("start","finish")
    val stdSliceRange2=  "StandardFamily"|"key"\\("start","finish").iterate // needs to be in the conversions??
    val stdPredicate =   "StandardFamily"|"key"\Array("column","column3","column3").results

    // Counter column family operations
    val countKey =         "CountFamily"|"key"
    val countColumnName =  "CountFamily"|"key"|"column"

    // get and add/subtract
    val countColumnValue = "CountFamily"|"key"|"column".count
    "CounterFamily"|"key"|"column" += 5
    "CounterFamily"|"key"|"column" -= 2

    val countSliceRange =  "CountFamily"|"key"\\("start","finish").results
    val countSliceRange2=  "CountFamily"|"key"\\("start","finish").iternate
    val countPredicate =   "CountFamily"|"key"\Array("column","column3","column3").results


    // SuperColumn family operations
    val superKey =         "SuperFamily"|"key"
    val superColumnSCol =  "SuperFamily"|"key"|"superColumn"

    // get and set
    val superColumnName =  "SuperFamily"|"key"|"superColumn"|"column".valueAs[String]
    "SuperFamily"|"key"|"superColumn"|"column" <= "New Vaue"

    val superColumnPred =  "SuperFamily"|"key"\Array("column", "column2").results
    val superColumnSlice = "SuperFamily"|"key"\\("start", "end").results
    val superColumnSlice4= "SuperFamily"|"key"\\("start", "end").iterate

    val superColumnPred2=  "SuperFamily"|"key"|"superColumn"\("column", "column2").results
    val superColumnSlice2= "SuperFamily"|"key"|"superColumn"\\("start", "end").results
    val superColumnSlice3= "SuperFamily"|"key"|"superColumn"\\("start", "end").iterate
  }
}