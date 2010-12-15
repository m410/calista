package org.brzy.calista

import org.scalatest.junit.JUnitSuite
import org.junit.Test
import org.junit.Assert._
import schema.{Types, LongType}

class LongTypeTest extends JUnitSuite {
  @Test def testLongType = {
    def buf = LongType.toBytes(500L)
    assertEquals(8,buf.array.length)
  }

  @Test def testTypes = {
    def buf = Types.toBytes(500L)
    assertEquals(8,buf.array.length)
    val lng = buf.getLong
    assertEquals(500,lng)
  }
}