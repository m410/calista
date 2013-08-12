package org.brzy.calista.schema

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.WordSpec
import org.brzy.calista.server.EmbeddedTest
import org.brzy.calista.system.{ColumnDefinition, FamilyDefinition}
import org.apache.commons.codec.binary.Hex
import java.nio.charset.Charset


class FamilyDefinitionSpec extends WordSpec with ShouldMatchers with EmbeddedTest {
  "FamilyDefinition" should {
    "be read" in {
      sessionManager.doWith { s=>
        val famDef = StandardFamily("Standard").definition
        assert(famDef != null)
        famDef.name should be equals("Standard")
      }
    }
    "be able to create new families" in {
      sessionManager.doWith { s=>
        val famDef = new FamilyDefinition(
          keyspace = "Test",
          name = "create_column_family",
          comparatorType = Option("UTF8Type") ,
          defaultValidationClass = Option("UTF8Type"),
          keyValidationClass = Option("UTF8Type"),
          columnMetadata = Some(List(
            new ColumnDefinition(
              name = "c_name".getBytes(Charset.forName("UTF-8")),
              validationClass = "UTF8Type",
              indexName = None,
              indexType = None
            )
          ))
        )
//        val created = famDef.create
//        assert(created != null)
//
//        sessionManager.doWith { s=>
//          val rows = Cql.query("select * from create_column_family where key='none'")
//          println("######## rows="+rows)
//          assert(rows.size == 1)
//        }
      }
    }
  }
}
