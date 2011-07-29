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

import org.brzy.calista.Calista
import org.brzy.calista.server.EmbeddedTest
import org.brzy.calista.serializer.{UTF8Serializer,IntSerializer,DateSerializer}
import java.util.Date
import org.brzy.calista.schema.StandardKey

class StandardMappingTest extends JUnitSuite  with EmbeddedTest {
	val personKey  = "mappingKey"
	val personDate = new Date
	
  @Test def mapEntity() {
	 	sessionManager.doWith { session =>
			Calista.value = Option(session)
			val person = new Person(personKey,"name",100,personDate)
			person.insert()
		}

	 	sessionManager.doWith { session =>
       import org.brzy.calista.dsl.Conversions._
       val key = "Person" | personKey
       val columns = session.list(key.asInstanceOf[StandardKey[String]])
       assertNotNull(columns)
       assertEquals(3,columns.size)
		}

		sessionManager.doWith { session =>
			Calista.value = Option(session)
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

			Calista.value = None
		}
  }
}

case class Person(key:String,name:String,count:Int,created:Date)

object Person extends StandardDao[String,Person] {
  def mapping = Mapping[Person](
      "Person",
      UTF8Serializer,
			Key("key"),
      Column("name"),
      Column("count",IntSerializer),
      Column("created",DateSerializer))
}