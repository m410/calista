package org.brzy.calista.schema

import org.brzy.calista.Calista
import org.brzy.calista.results.Row

/**
 * Executes arbitrary CQL.
 *
 * @see http://cassandra.apache.org/doc/cql/CQL.html#CassandraQueryLanguageCQLv2.0
 * 
 * @author Michael Fortin
 */
object Cql {
  def query(query:String):Seq[Row] = {
    Calista.value.query(query)
  }
}
