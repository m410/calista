package org.brzy.calista

import java.util.UUID
import org.scalatest.junit.JUnitSuite
import results.{SuperColumn, Column}
import schema.Utf8Type
import server.EmbeddedTest
import org.junit.Test
import org.junit.Assert._

// create a single column, save it, and get it out
class GetSetTest extends JUnitSuite with EmbeddedTest {
//  val sessionManager = new SessionManager()
  
  @Test def testSetAndGetStandardColumn = {
    import column.Conversions._

    val key = "Standard" |   "testKey" // UUID.randomUUID //

    sessionManager.doWith { session =>
      session.insert(key | ("column", "value"))
    }

    sessionManager.doWith { session =>
      val result = session.get(key | ("column"))
      assertNotNull(result)
      assertTrue(result.isDefined)
      assertEquals("value", result.get.asInstanceOf[Column].valueAs(Utf8Type))
      assertEquals("column", result.get.asInstanceOf[Column].nameAs(Utf8Type))
    }
  }

  // create a single column, save it, and get it out
  @Test def testSetAndGetSuperColumn = {
    import column.Conversions._
    val superColumn = "Super" |^ "shouldBeUUID"/*UUID.randomUUID*/ | "12345L"

    sessionManager.doWith { session =>
      val column = superColumn | ("column", "value")
      session.insert(column)
    }

    sessionManager.doWith { session =>
			val getColumn = superColumn | ("column")
      val result = session.get(getColumn)
      assertNotNull(result)
      assertTrue(result.isDefined)
      assertEquals("value", result.get.asInstanceOf[Column].valueAs(Utf8Type))
      assertEquals("column", result.get.asInstanceOf[Column].nameAs(Utf8Type))
    }
  }
}