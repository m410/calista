package org.brzy.calista

import org.scalatest.junit.JUnitSuite
import org.junit.Test
import org.junit.Assert._
import server.EmbeddedTest


class SessionManagerTest extends JUnitSuite with EmbeddedTest {
  @Test def testSchema = {
    val sm = new SessionManager
    val schema = sm.keyspaceDefinition
    assertNotNull(schema)
    println(schema)
  }
}