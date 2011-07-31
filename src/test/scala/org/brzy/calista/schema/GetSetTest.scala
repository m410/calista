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
import org.brzy.calista.server.EmbeddedTest
import org.junit.Test
import org.junit.Assert._
import java.util.UUID
import org.brzy.calista.dsl.Conversions
import org.brzy.calista.results.Row

class GetSetTest extends JUnitSuite with EmbeddedTest {

  @Test def testSetAndGetStandardColumn() {
    import Conversions._
    val key = "Standard" | "testKey"

    sessionManager.doWith { session =>
      session.insert(key | ("column", "value"))
    }

    sessionManager.doWith { session =>
      val result = session.get(key | ("column",null))
      assertNotNull(result)
      assertTrue(result.isDefined)
      val row = result.get
      assertNotNull(row.rowType)
      assertNotNull(row.family)
      assertNotNull(row.key)
      assertNull(row.superColumn)
      assertNotNull(row.column)
      assertNotNull(row.value)
      assertNotNull(row.timestamp)
      assertEquals("value", row.valueAs[String])
      assertEquals("column", row.columnAs[String])
    }
  }

  // create a single column, save it, and get it out
  @Test def testSetAndGetSuperColumn() {
    import Conversions._
    val superColumn = "Super" || "superKey" | 12345L
		val columnName = UUID.randomUUID

    sessionManager.doWith { session =>
      val column = superColumn | (columnName, "value")
      session.insert(column)
    }

    sessionManager.doWith { session =>
			val cName = superColumn | columnName
      val result = session.get(cName.asInstanceOf[ColumnName[UUID]].asColumn)
      assertNotNull(result)
      assertTrue(result.isDefined)
      val column = result.get.asInstanceOf[Row]
      assertEquals("value", column.valueAs[String])
      assertEquals(columnName.toString, column.columnAs[UUID].toString)
    }
  }
}