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
import org.brzy.calista.schema.{Utf8Type,IntType,DateType}
import java.util.Date

class ColumnMappingTest extends JUnitSuite  with EmbeddedTest {
  @Test def mapEntity = {
	 	sessionManager.doWith { session =>
			Calista.value = Option(session)
			val person = new Person("abc","name",100,new Date)
			person.save
			Calista.value = None
		}
		
		sessionManager.doWith { session =>
			Calista.value = Option(session)
			val person = Person.get("abc")
			assertNotNull(person)
			assertNotNull(person.key)
			assertNotNull(person.name)
			assertNotNull(person,count)
			assertNotNull(person,date)
			Calista.value = None
		}
  }
}

case class Person(key:String,name:String,count:Int,created:Date)  extends KeyedEntity[String]

object Person extends Dao[String,Person] {
	def columnMapping = new ColumnMapping[Person]("Person")
			.attributes(Utf8Type, List(
				Attribute("name"),
				Attribute("count",IntType),
				Attribute("created",DateType)))
}