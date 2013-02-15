package org.brzy.calista.ocm

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

import org.brzy.calista.serializer.{DateSerializer, IntSerializer}
import org.brzy.calista.schema.StandardFamily
import org.brzy.calista.server.EmbeddedTest

import java.util.Date


class StandardBuilderMappingSpec extends WordSpec with ShouldMatchers with EmbeddedTest {

  val personKey = "mappingBuilderKey"
  val personKeyPartial = "mappingBuilderKeyPartial"
  val personDate = new Date

  "Standard Builder" should {
    "make instance" in {
      sessionManager.doWith {
        session =>
          val person = new Person(personKey, "name", 100, personDate)
          person.insert()
      }

      sessionManager.doWith {
        session =>
          val key = StandardFamily("Person")(personKey)
          val columns = key.list
          assert(null != columns)
          assert(3 ==  columns.size)
      }

      sessionManager.doWith {
        session =>
          Person.get(personKey) match {
            case Some(person) =>
              assert(null != person)
              assert(null != person.key)
              assert(null != person.name)
              assert(null != person.count)
              assert(null != person.created)
            case _ =>
              assert(false,"No result for get")
          }
      }
    }
  }
}

case class PersonBuild(key: String, name: String, count: Int, created: Date)

object PersonBuild extends StandardDao[String, PersonBuild] {


  val mapping = new StandardMappingBuilder[String,PersonBuild]()
      .family("Person")
      .key("key")
      .column("name")
      .column("count",IntSerializer)
      .column("created",DateSerializer)
      .make
}
