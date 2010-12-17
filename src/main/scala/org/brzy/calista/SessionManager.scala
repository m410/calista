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
class SessionManager(url:String = "localhost", port:Int = 9160) {
  val keyspace = "Test"
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