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
import org.brzy.calista.schema.StandardFamily

import java.util.Date
import scala.reflect.runtime.universe.TypeTag


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
        Person.get(personKey) match {
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
        val person = new Person(personKeyPartial, "name", 100, new Date)
        person.insert()
    }

    sessionManager.doWith {
      session =>
        val key = StandardFamily("Person")(personKeyPartial)
        val columns = key.list
        assertNotNull(columns)
        assertEquals(3, columns.size)
    }

    sessionManager.doWith {
      session =>
        Person.get(personKeyPartial) match {
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
}

case class Person(key: String, name: String, count: Int, created: Date)

object Person extends StandardDao[String, Person] {


  val mapping = new StandardMapping[String,Person] {
    val family = "Person"
    def keyFor(t: Person) = t.key

    def newInstance(k: String) = {
      val rows = StandardFamily(family)(k).list
      rows.foreach(r=>println(r.columnAs[String]))

      Person(
        key = k,
        name = rows.find(_.columnAs[String] == "name") match {
          case Some(r) => r.valueAs[String]
          case _ => null
        },
        count = rows.find(_.columnAs[String] == "count") match {
          case Some(r) => r.valueAs[Int]
          case _ => 0
        },
        created = rows.find(_.columnAs[String] == "created") match {
          case Some(r) => new Date(r.valueAs[Long])
          case _ => null
        }
      )
    }

    def toColumns(instance: Person)(implicit t:TypeTag[Person]) = {
      List(
        StandardFamily(family)(instance.key).column("name",instance.name),
        StandardFamily(family)(instance.key).column("count",instance.count),
        StandardFamily(family)(instance.key).column("created",instance.created.getTime)
      )
    }
  }
}