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
package org.brzy.calista.ocm

import org.scalatest.junit.JUnitSuite
import org.junit.Test
import org.junit.Assert._

import org.brzy.calista.server.EmbeddedTest
import org.brzy.calista.serializer.{UTF8Serializer, IntSerializer, DateSerializer}
import java.util.Date
import org.brzy.calista.schema.StandardFamily

class StandardMappingTest extends JUnitSuite with EmbeddedTest {
  val personKey = "mappingKey"
  val personKeyPartial = "mappingKeyPartial"
  val personDate = new Date

  @Test def mapEntity() {
    sessionManager.doWith {
      session =>
        val person = new Person(personKey, "name", 100, personDate)
        person.insert()
    }

    sessionManager.doWith {
      session =>
        val key = StandardFamily("Person")(personKey)
        val columns = key.list
        assertNotNull(columns)
        assertEquals(3, columns.size)
    }

    sessionManager.doWith {
      session =>
      val person = Person.get(personKey) match {
        case Some(person) =>
          assertNotNull(person)
          assertNotNull(person.key)
          assertNotNull(person.name)
          assertNotNull(person.count)
          assertNotNull(person.created)
        case _ =>
          fail("No person by key")
      }
    }
  }

  @Test def mapPartialEntity() {
    sessionManager.doWith {
      session =>
        val person = new Person(personKeyPartial, "name", 100, null)
        person.insert()
    }

    sessionManager.doWith {
      session =>
        val key = StandardFamily("Person")(personKeyPartial)
        val columns = key.list
        assertNotNull(columns)
        assertEquals(2, columns.size)
    }

    sessionManager.doWith {
      session =>
        Person.get(personKeyPartial) match {
          case Some(person) =>
            assertNotNull(person)
            assertNotNull(person.key)
            assertNotNull(person.name)
            assertNotNull(person.count)
            assertNull(person.created)
          case _ =>
            fail("No person by key")
        }
    }
  }
}

case class Person(key: String, name: String, count: Int, created: Date)

object Person extends StandardDao[String, Person] {
  def mapping = BeanMapping[Person](
    "Person",
    UTF8Serializer,
    Key("key"),
    Column("name"),
    Column("count", IntSerializer),
    Column("created", DateSerializer))
}