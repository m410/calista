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
package org.brzy.calista

import org.apache.thrift.transport.{TSocket, TFramedTransport}
import org.apache.cassandra.thrift.Cassandra
import org.apache.thrift.protocol.TBinaryProtocol
import collection.JavaConversions._
import schema.{FamilyDefinition, KeyspaceDefinition}

/**
 * Document Me..
 *
 * @author Michael Fortin
 */
class SessionManager(keyspace:String = "Test", url:String = "localhost", port:Int = 9160) {
  val host = Host(url, port, 250)
  // todo setup host from configuration
  // todo load or modify schema

  lazy val keyspaceDefinition = {
    val sock = new TFramedTransport(new TSocket(host.address, host.port, host.timeout))
    val protocol = new TBinaryProtocol(sock)
    val client = new Cassandra.Client(protocol, protocol)
    sock.open

    try {
      val ksdef = client.describe_keyspace(keyspace)
      KeyspaceDefinition(ksdef.name,
      ksdef.strategy_class,
      ksdef.replication_factor,
      ksdef.cf_defs.map(cfdef=>{
        FamilyDefinition(cfdef.name,
          cfdef.column_type,
          cfdef.comparator_type,
          cfdef.subcomparator_type,
          cfdef.comment,
          cfdef.row_cache_size,
          cfdef.key_cache_size)}
        ).toList
      )
    }
    finally {
      sock.close
    }
  }

  lazy val clusterName = {
    val sock = new TFramedTransport(new TSocket(host.address, host.port, host.timeout))
    val protocol = new TBinaryProtocol(sock)
    val client = new Cassandra.Client(protocol, protocol)
    sock.open

    try {
      client.describe_cluster_name
    }
    finally {
      sock.close
    }
  }

  lazy val version = {
    val sock = new TFramedTransport(new TSocket(host.address, host.port, host.timeout))
    val protocol = new TBinaryProtocol(sock)
    val client = new Cassandra.Client(protocol, protocol)
    sock.open

    try {
      client.describe_version
    }
    finally {
      sock.close
    }
  }
  
  def createSession = new Session(host,keyspaceDefinition)

  def doWith(f: (Session) => Unit) = {
    val session = createSession
    f(session)
    session.close
  }
}

case class Host(address: String, port: Int, timeout: Int)

