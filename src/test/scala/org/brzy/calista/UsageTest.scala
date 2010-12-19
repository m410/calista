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
  @Test def testDsl = {
    import Conversions._

    val standardKey:StandardKey[String] =
        "Family" | "key"
    
    val standardKeyWithUUID:StandardKey[UUID] =
        "Family" | UUID.randomUUID

    val standardKeyWithLong:StandardKey[Long] =
        "Family" | 22343L

    val standardColumn:Column[String,String] =
        "Family" | "key" |("column", "value")
//    val standardColumn = ColumnFamily("Family").key("key").column("column", "value")

    val standardKeyRange:KeyRange[String,String] =
        "Family" \("start","end",List.empty[String],20)

    val standardSliceRange:SliceRange[String] =
        {"Family" | "key"}\("start","finish")

    val standardSlicePredicate:SlicePredicate[String] =
        {"Family"|"key"}\\List("column","column3","column3")
//    val ssp2 = ColumnFamily("Family").key("key").predicate(List("column","column3","column3"))

    val superKey:SuperKey[String] =
        "Super" |^ "SuperKey"

    val superColumn:SuperColumn[String] =
        "Super" |^ "SuperKey" | "SuperColumn"

    val superColumnVal:Column[String,String] =
        "Super" |^ "SuperKey" | "SuperColumn" |("column", "value")

//    val superSlicePredicate = "Super" |^ "key" \\("column","column1")
//    val superSlicePredicate2 = "Super" |^ "key" | "SuperColumn" \\("column","column1")
//    val superSliceRange = "Super" |^ "key" \("start","finish",true,10)
//    val superSliceRange2 = "Super" |^ "key" | "SuperColumn" \("start","finish",true,10)

//    "Super-s" |^ "SuperKey" | "SuperColumn" |("column", "value").save
  }
}