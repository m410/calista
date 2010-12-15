package org.brzy.calista

import column.KeyRange
import schema.Utf8Type
import server.EmbeddedTest
import org.scalatest.junit.JUnitSuite
import org.junit.Test
import org.junit.Assert._


class RangeTest extends JUnitSuite with EmbeddedTest {

  @Test def testPredicateOnStandard = {
    val sessionManager = new SessionManager()
    import column.Conversions._

    sessionManager.doWith { session =>
      session.insert("Standard"|"key-range-0" | ("column", "value0"))
      session.insert("Standard"|"key-range-1" | ("column", "value1"))
      session.insert("Standard"|"key-range-2" | ("column", "value2"))
      session.insert("Standard"|"key-range-3" | ("column", "value3"))
      session.insert("Standard"|"key-range-4" | ("column", "value4"))
    }

    sessionManager.doWith { session =>
      val standardKeyRange:KeyRange[String,String] = "Standard" \("key-range-4","key-range",List("column"))
      val keys = session.get(standardKeyRange)
      keys.keySet.foreach(k=>println("##k='%s'".format(k)))
      println(keys)
      assertNotNull(keys)
      assertEquals(1,keys.size)
    }
  }
}