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
import org.brzy.calista.serializer.{UTF8Serializer,IntSerializer,DateSerializer}
import java.util.Date

class ColumnMappingTest extends JUnitSuite  with EmbeddedTest {
	val personKey  = "mappingKey"
	val personDate = new Date
	
  @Test def mapEntity = {
	 	sessionManager.doWith { session =>
			Calista.value = Option(session)
			val person = new Person(personKey,"name",100,personDate)
			person.save
			Calista.value = None
		}

	 	sessionManager.doWith { session =>
       import org.brzy.calista.schema.Conversions._
       val key = "Person" | personKey
       val columns = session.list(key)
       assertNotNull(columns)
       assertEquals(3,columns.size)
		}

		sessionManager.doWith { session =>
			Calista.value = Option(session)
			val person = Person.get(personKey)
			assertNotNull(person)
			assertNotNull(person.key)
			assertNotNull(person.name)
			assertNotNull(person.count)
			assertNotNull(person.created)
			Calista.value = None
		}
  }
}

case class Person(key:String,name:String,count:Int,created:Date)  extends KeyedEntity[String]

object Person extends Dao[String,Person] {
	def columnMapping = new ColumnMapping[Person]("Person")
			.attributes(UTF8Serializer, Array(
				Attribute("key",true),
				Attribute("name"),
				Attribute("count",false,IntSerializer),
				Attribute("created",false,DateSerializer)))
}