package org.brzy.calista.schema

import org.brzy.calista.server.EmbeddedTest
import org.scalatest.junit.JUnitSuite
import org.junit.Test
import org.junit.Assert._
import org.brzy.calista.serializer.UTF8Serializer
import org.brzy.calista.dsl.Conversions


class ScrollTest extends JUnitSuite with EmbeddedTest {

  @Test def smallScroll() {
    import Conversions._

    sessionManager.doWith { session =>
      session.insert("Standard"|"key-range-0"|("column0", "value0"))
      session.insert("Standard"|"key-range-0"|("column1", "value1"))
      session.insert("Standard"|"key-range-0"|("column2", "value2"))
      session.insert("Standard"|"key-range-0"|("column3", "value3"))
      session.insert("Standard"|"key-range-0"|("column4", "value4"))
      session.insert("Standard"|"key-range-0"|("column5", "value5"))
      session.insert("Standard"|"key-range-0"|("column6", "value6"))
      session.insert("Standard"|"key-range-0"|("column7", "value7"))
      session.insert("Standard"|"key-range-0"|("column8", "value8"))
      session.insert("Standard"|"key-range-0"|("column9", "value9"))
    }

    sessionManager.doWith { session =>
      val sliceRange = {"Standard"|"key-range-0"}\\("column2","column6")
      val iterator = session.scrollSliceRange(sliceRange.asInstanceOf[SliceRange[String]])
      var count = 0

      while(iterator.hasNext) {
        val next = iterator.next
        assertNotNull(next)
//        println("### next: " + next.nameAs(UTF8Serializer))
        count = count + 1
      }

      assertEquals(5,count)
    }
  }


  @Test def pagingScroll() {
    import Conversions._

    sessionManager.doWith { session =>
      session.insert("Standard"|"key-range-0"|("column0", "value0"))
      session.insert("Standard"|"key-range-0"|("column1", "value1"))
      session.insert("Standard"|"key-range-0"|("column2", "value2"))
      session.insert("Standard"|"key-range-0"|("column3", "value3"))
      session.insert("Standard"|"key-range-0"|("column4", "value4"))
      session.insert("Standard"|"key-range-0"|("column5", "value5"))
      session.insert("Standard"|"key-range-0"|("column6", "value6"))
      session.insert("Standard"|"key-range-0"|("column7", "value7"))
      session.insert("Standard"|"key-range-0"|("column8", "value8"))
      session.insert("Standard"|"key-range-0"|("column9", "value9"))
    }

    sessionManager.doWith { session =>
      val sliceRange = {"Standard"|"key-range-0"}\\("column8","column2",true,3)
      val iterator = session.scrollSliceRange(sliceRange.asInstanceOf[SliceRange[String]])
      var count = 0

      while(iterator.hasNext) {
        val next = iterator.next
        assertNotNull(next)
//        println("### next: " + next.nameAs(UTF8Serializer))
        count = count + 1
      }

      assertEquals(7,count)
    }
  }

  @Test def pagingMissingScroll() {
    import Conversions._

    sessionManager.doWith { session =>
      session.insert("Standard"|"key-range-0"|("column00", "value0"))
//      session.insert("Standard"|"key-range-0"|("column01", "value1"))
      session.insert("Standard"|"key-range-0"|("column02", "value2"))
      session.insert("Standard"|"key-range-0"|("column03", "value3"))
//      session.insert("Standard"|"key-range-0"|("column04", "value4"))
      session.insert("Standard"|"key-range-0"|("column05", "value5"))
      session.insert("Standard"|"key-range-0"|("column06", "value6"))
//      session.insert("Standard"|"key-range-0"|("column07", "value7"))
      session.insert("Standard"|"key-range-0"|("column08", "value8"))
      session.insert("Standard"|"key-range-0"|("column09", "value9"))
    }

    sessionManager.doWith { session =>
      val sliceRange = {"Standard"|"key-range-0"}\\("column01","column07")
      val iterator = session.scrollSliceRange(sliceRange)
      var count = 0

      while(iterator.hasNext) {
        val next = iterator.next
        assertNotNull(next)
//        println("### next: " + next.nameAs(UTF8Serializer))
        count = count + 1
      }

      assertEquals(4,count)
    }
  }



  @Test def emptyScroll() {
    import Conversions._

    sessionManager.doWith { session =>
      val sliceRange = {"Standard"|"key-range-0"}\\("column001","column007")
      val iterator = session.scrollSliceRange(sliceRange)
      var count = 0

      while(iterator.hasNext) {
        val next = iterator.next
        assertNotNull(next)
        count = count + 1
      }

      assertEquals(0,count)
    }
  }
}