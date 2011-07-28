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

    val standardKey = "Family"|"key"
    
    val standardKeyWithUUID = "Family"|UUID.randomUUID

    val standardKeyWithLong = "Family"|22343L

    val standardColumn = "Family"|"key"|("column", "value")

    val standardKeyRange = "Family"\("start","end",List.empty[String],20)

    val standardSliceRange = "Family"|"key"\("start","finish")

    val standardSlicePredicate = "Family"|"key"\\List("column","column3","column3")

    val superKey = "Super"|"SuperKey"

    val superColumn = "Super"|"SuperKey"|"SuperColumn"

    val superColumnVal = "Super"|"SuperKey"|"SuperColumn"|("column", "value")
  }
}