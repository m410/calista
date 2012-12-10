package org.brzy.calista.schema

import org.brzy.calista.server.EmbeddedTest
import org.scalatest.junit.JUnitSuite
import org.junit.Test
import org.junit.Assert._


class ScrollTest extends JUnitSuite with EmbeddedTest {

  @Test def smallScroll() {

    sessionManager.doWith { session =>
      StandardFamily("Standard")("key-range-0")("column0").set("value0")
      StandardFamily("Standard")("key-range-0")("column1").set("value1")
      StandardFamily("Standard")("key-range-0")("column2").set("value2")
      StandardFamily("Standard")("key-range-0")("column3").set("value3")
      StandardFamily("Standard")("key-range-0")("column4").set("value4")
      StandardFamily("Standard")("key-range-0")("column5").set("value5")
      StandardFamily("Standard")("key-range-0")("column6").set("value6")
      StandardFamily("Standard")("key-range-0")("column7").set("value7")
      StandardFamily("Standard")("key-range-0")("column8").set("value8")
      StandardFamily("Standard")("key-range-0")("column9").set("value9")
    }

    sessionManager.doWith { session =>
      val sliceRange = StandardFamily("Standard")("key-range-0").from("column2").to("column6")
      assertEquals(sliceRange.finish.get ,"column6")
      assertEquals(sliceRange.start.get , "column2")
      val iterator = sliceRange.iterator
      var count = 0

      while(iterator.hasNext) {
        val next = iterator.next()
        println("##column value="+next.valueAs[String])
        assertNotNull(next)
        count = count + 1
      }
      assertEquals(5,count)
    }
  }

  @Test def pagingScroll() {
    sessionManager.doWith { session =>
      StandardFamily("Standard")("key-range-0")("column0").set("value0")
      StandardFamily("Standard")("key-range-0")("column1").set("value1")
      StandardFamily("Standard")("key-range-0")("column2").set("value2")
      StandardFamily("Standard")("key-range-0")("column3").set("value3")
      StandardFamily("Standard")("key-range-0")("column4").set("value4")
      StandardFamily("Standard")("key-range-0")("column5").set("value5")
      StandardFamily("Standard")("key-range-0")("column6").set("value6")
      StandardFamily("Standard")("key-range-0")("column7").set("value7")
      StandardFamily("Standard")("key-range-0")("column8").set("value8")
      StandardFamily("Standard")("key-range-0")("column9").set("value9")
    }

    sessionManager.doWith { session =>
      val sliceRange = StandardFamily("Standard")("key-range-0").from("column8").to("column2").reverse.size(3)
      val iterator = sliceRange.iterator
      var count = 0

      while(iterator.hasNext) {
        val next = iterator.next()
        assertNotNull(next)
        count = count + 1
      }
      assertEquals(7,count)
    }
  }

  @Test def pagingMissingScroll() {
    sessionManager.doWith { session =>
      StandardFamily("Standard")("key-range-0")("column00").set("value0")
      StandardFamily("Standard")("key-range-0")("column02").set("value2")
      StandardFamily("Standard")("key-range-0")("column03").set("value3")
      StandardFamily("Standard")("key-range-0")("column05").set("value5")
      StandardFamily("Standard")("key-range-0")("column06").set("value6")
      StandardFamily("Standard")("key-range-0")("column08").set("value8")
      StandardFamily("Standard")("key-range-0")("column09").set("value9")
    }

    sessionManager.doWith { session =>
      val sliceRange = StandardFamily("Standard")("key-range-0").from("column01").to("column07")
      val iterator = session.scrollSliceRange(sliceRange)
      var count = 0

      while(iterator.hasNext) {
        val next = iterator.next()
        assertNotNull(next)
        count = count + 1
      }
      assertEquals(4,count)
    }
  }

  @Test def emptyScroll() {
    sessionManager.doWith { session =>
      val sliceRange = StandardFamily("Standard")("key-range-0").from("column001").to("column007")
      val iterator = session.scrollSliceRange(sliceRange)
      var count = 0

      while(iterator.hasNext) {
        val next = iterator.next()
        assertNotNull(next)
        count = count + 1
      }
      assertEquals(0,count)
    }
  }
}