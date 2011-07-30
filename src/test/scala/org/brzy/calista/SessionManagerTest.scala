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
package org.brzy.calista

import org.scalatest.junit.JUnitSuite
import org.junit.Test
import org.junit.Assert._
import server.EmbeddedTest
import org.slf4j.LoggerFactory
import system.{FamilyDefinition, KeyspaceDefinition}

class SessionManagerTest extends JUnitSuite with EmbeddedTest {
  val log = LoggerFactory.getLogger(getClass)

  @Test def testSchema() {
//    val mgr = new SessionManager("Test", "127.0.0.1")
//    mgr.doWith({
//      session =>
//        log.info("******************** Before add keyspace")
//        session.addKeyspace(KeyspaceDefinition(
//          name = "Test",
//          strategyClass = "org.apache.cassandra.locator.SimpleStrategy",
//          families = List.empty[FamilyDefinition]))
//        log.info("********************")
//        session.addColumnFamily(new FamilyDefinition(
//          keyspace = "Test",
//          name = "Standard",
//          comparatorType = Option("UTF8Type")))
//        log.info("********************")
//        session.addColumnFamily(new FamilyDefinition(
//          keyspace = "Test",
//          name = "StandardFamily",
//          comparatorType = Option("UTF8Type")))
//        log.info("********************")
//        session.addColumnFamily(new FamilyDefinition(
//          keyspace = "Test",
//          name = "Person",
//          comparatorType = Option("UTF8Type")))
//        log.info("********************")
//        session.addColumnFamily(new FamilyDefinition(
//          keyspace = "Test",
//          name = "SuperFamily",
//          columnType = "Super",
//          comparatorType = Option("UTF8Type"),
//          subcomparatorType = Option("UTF8Type")))
//        log.info("********************")
//        session.addColumnFamily(new FamilyDefinition(
//          keyspace = "Test",
//          name = "CountFamily",
//          columnType = "Standard",
//          defaultValidationClass = Option("CounterColumnType"),
//          comparatorType = Option("UTF8Type")))
//        log.info("********************")
//        session.addColumnFamily(new FamilyDefinition(
//          keyspace = "Test",
//          name = "SPerson",
//          columnType = "Super",
//          comparatorType = Option("UTF8Type"),
//          subcomparatorType = Option("UTF8Type")))
//        log.info("********************")
//        session.addColumnFamily(new FamilyDefinition(
//          keyspace = "Test",
//          name = "Super",
//          columnType = "Super",
//          comparatorType = Option("LongType"),
//          subcomparatorType = Option("LexicalUUIDType")))
//        log.info("********************")
//        session.addColumnFamily(new FamilyDefinition(
//          keyspace = "Test",
//          name = "Super2",
//          columnType = "Super",
//          comparatorType = Option("UTF8Type"),
//          subcomparatorType = Option("UTF8Type")))
//
//    })

    val sm = new SessionManager("Test", "127.0.0.1")
    val schema = sm.keyspaceDefinition
    assertNotNull(schema)
    println(schema)

  }
}