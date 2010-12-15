package org.brzy.calista

import org.junit.Test
import org.junit.Assert._
import server.EmbeddedTest
import org.scalatest.junit.JUnitSuite


class CountTest extends JUnitSuite with EmbeddedTest {
  @Test def countTest = {
    import column.Conversions._
    val key = "Standard" | "count"

    sessionManager.doWith { session =>
      session.insert(key | ("column5", "value0"))
      session.insert(key | ("column4", "value1"))
      session.insert(key | ("column3", "value2"))
      session.insert(key | ("column2", "value3"))
      session.insert(key | ("column1", "value4"))
    }

    sessionManager.doWith { session =>
      val amount = session.count(key)
      assertNotNull(amount)
      assertEquals(5,amount)
    }
  }
}