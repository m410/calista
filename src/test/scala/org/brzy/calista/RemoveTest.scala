package org.brzy.calista

import org.scalatest.junit.JUnitSuite
import server.EmbeddedTest
import org.junit.Test
import org.junit.Assert._


class RemoveTest extends JUnitSuite with EmbeddedTest {
  @Test def removeTest = {
    import column.Conversions._
    
    val key = "Standard" | "remover"
    sessionManager.doWith { session =>
        session.insert(key | ("column5", "value0"))
    }

    sessionManager.doWith { session =>
      session.remove(key|("column5"))
    }

    sessionManager.doWith { session =>
      val result = session.get(key | ("column5"))
      assertNotNull(result)
      assertTrue(result.isEmpty)
    }
  }
}