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
import org.brzy.calista.Session
import org.brzy.calista.schema.Conversions


class SuperMappingTest extends JUnitSuite  with EmbeddedTest {
  val familyName = "SPerson"
	val personKey  = "super_key"
	val personSuperColumn = "super_column"
	val personDate = new Date
	
  @Test def mapEntity() {
	 	sessionManager.doWith { session =>
			Calista.value = Option(session)
			val person = new SPerson(personKey,personSuperColumn,"name",100,personDate)
			person.insert
		}

	 	sessionManager.doWith { session =>
       import org.brzy.calista.schema.Conversions._
       val key = familyName |^ personKey | personSuperColumn
       val columns = session.list(key)
       assertNotNull(columns)
       assertEquals(3,columns.size)
		}

		sessionManager.doWith { session =>
			Calista.value = Option(session)
			val person = SPerson.get(personKey, personSuperColumn) match {
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
    sessionManager.doWith { (session:Session) =>
      import Conversions._
      val columns = session.list(familyName|^personKey)
      assertNotNull(columns)
      assertEquals(1,columns.size)
    }
  }
}

case class SPerson(key:String,
    superColumn:String,
    name:String,
    count:Int,
    created:Date)
    extends SuperEntity[String,String]

object SPerson extends SuperDao[String,String,SPerson] {
  def mapping = new Mapping[SPerson](
      "SPerson",
      UTF8Serializer,
			Key(),
      SuperColumn(),
      Column("name"),
      Column("count",IntSerializer),
      Column("created",DateSerializer))
}