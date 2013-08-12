package org.brzy.calista.schema

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers
import org.brzy.calista.server.EmbeddedTest


class CqlSpec extends WordSpec with ShouldMatchers with EmbeddedTest{
  "Cql" should {
    "be able to select rows" ignore {
      sessionManager.doWith { s=>
        val rows = Cql.query("select * from create_column_family where key='none'")
        assert(rows.size == 0)
      }
    }
    "be able to insert rows" in {

    }
  }
}
