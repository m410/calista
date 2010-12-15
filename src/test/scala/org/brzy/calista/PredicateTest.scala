package org.brzy.calista

import server.EmbeddedTest
import org.scalatest.junit.JUnitSuite
import org.junit.Test
import org.junit.Assert._


class PredicateTest extends JUnitSuite with EmbeddedTest  {

  @Test def testPredicateOnStandard = {
    import column.Conversions._
    val key = "Standard"|"predicate-key"

    sessionManager.doWith { session =>
      session.insert(key | ("column0", "value0"))
      session.insert(key | ("column1", "value1"))
      session.insert(key | ("column2", "value2"))
      session.insert(key | ("column3", "value3"))
      session.insert(key | ("column4", "value4"))
    }

    sessionManager.doWith { session =>
      val standardSlicePredicate = key\\ List("column1","column2","column3")
      val columns = session.get(standardSlicePredicate)
      assertNotNull(columns)
      assertEquals(3,columns.size)
    }
  }  
}