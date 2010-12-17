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
			val person = new Person("abc","name",100,new Date)
			person.save
		}
		
		sessionManager.doWith { session =>
			val person = Person.get("abc")
			assertNotNull(person)
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