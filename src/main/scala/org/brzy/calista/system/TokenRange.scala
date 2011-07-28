package org.brzy.calista.system

import org.apache.cassandra.thrift.{TokenRange=>CassandraTokenRange}
import collection.JavaConversions._

/**
 * Datasore meta data respenting the nodes in the cluster.
 * {{{
 * struct TokenRange {
 *     1: required string start_token,
 *     2: required string end_token,
 *     3: required list<string> endpoints,
 * }
 * }}}
 * @author Michael Fortin
 */
case class TokenRange(
    startToken:String,
    endToken:String,
    endpoints:List[String])

object TokenRange {
  def apply(tr:CassandraTokenRange) = {
    new TokenRange(
      startToken = tr.getStart_token,
      endToken = tr.getEnd_token,
      endpoints = tr.getEndpoints.toList
    )
  }
}