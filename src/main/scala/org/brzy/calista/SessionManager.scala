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

import org.apache.cassandra.thrift.Cassandra

import org.apache.thrift.transport.{TSocket, TFramedTransport}
import org.apache.thrift.protocol.TBinaryProtocol

import collection.JavaConversions._
import system.{FamilyDefinition, KeyspaceDefinition}

/**
 * Session Manager works in a similar fashion as the Entity Manager Factory  in JPA.  It's 
 * partially responsible for the session life cycle.  It it also can provide basic information
 * about the datastore it's configured to connect too.
 *
 * @todo Authentication is not used or implemented.
 * @todo Only connects to a single host, multiple host connections is not implemented.
 * @todo Can not modifiy the schema.
 * @todo The timeout needs to be configurable.
 *
 * @author Michael Fortin
 */
class SessionManager(keyspace:String = "Test", url:String = "localhost", port:Int = 9160) {

	/**
	 * the host and port for where this session manager connects.
	 */
  val host = Host(url, port, 2500)

	/**
	 * Outputs the keyspace definition.  This will output the keyspace, families and their attributes.
	 */
  lazy val keyspaceDefinition = {
    val sock = new TFramedTransport(new TSocket(host.address, host.port, host.timeout))
    val protocol = new TBinaryProtocol(sock)
    val client = new Cassandra.Client(protocol, protocol)
    sock.open()

    try {
      KeyspaceDefinition(client.describe_keyspace(keyspace))
    }
    finally {
      sock.close()
    }
  }

	/**
	 * Ouputs the cluster named provided by the datastore.
	 */
  lazy val clusterName = {
    val sock = new TFramedTransport(new TSocket(host.address, host.port, host.timeout))
    val protocol = new TBinaryProtocol(sock)
    val client = new Cassandra.Client(protocol, protocol)
    sock.open()

    try {
      client.describe_cluster_name
    }
    finally {
      sock.close()
    }
  }

	/**
	 * Outputs the version of the cassandra server
	 */
  lazy val version = {
    val sock = new TFramedTransport(new TSocket(host.address, host.port, host.timeout))
    val protocol = new TBinaryProtocol(sock)
    val client = new Cassandra.Client(protocol, protocol)
    sock.open()

    try {
      client.describe_version
    }
    finally {
      sock.close()
    }
  }
  
//	/**
//	 * Tells the cassandra server to load a configuration from it's own configuration file.  As of version
//	 * 7 of cassandra, the configurations are not automatically loaded, you have to do it via
//	 * cassandera-cli tool or via this api.
//	 */
//	def loadSchemaFromConfig = {
//    import collection.JavaConversions._
//
//    for (ksm: KSMetaData <- DatabaseDescriptor.readTablesFromYaml()) {
//      for (cfm: CFMetaData <- ksm.cfMetaData().values())
//        CFMetaData.map(cfm)
//      DatabaseDescriptor.setTableDefinition(ksm, DatabaseDescriptor.getDefsVersion())
//    }
//  }

	/**
	 * This creates a new session to interact with cassandra using the configured keyspaceDefinition and
	 * host.  Using this factory method required you to manage the life cycle your self. I will lazyly
	 * open a connect to cassandra but it will not close it.
	 * 
	 * @see Session
	 */
  def createSession = new Session(host,keyspaceDefinition)

	/**
	 * Manaages the life cycle of a session automatically.
	 */
  def doWith(f: (Session) => Unit) {
    val session = createSession
    f(session)
    session.close()
  }
}

/**
 * A Host instance for cassandera to connect too.
 */
case class Host(address: String, port: Int, timeout: Int)

