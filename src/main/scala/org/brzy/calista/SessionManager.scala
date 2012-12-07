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

import system.{FamilyDefinition, KeyspaceDefinition}
import org.slf4j.LoggerFactory

/**
 * SessionImpl Manager works in a similar fashion as the Entity Manager Factory  in JPA.  It's
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
class SessionManager(keyspace: String, url: String, port: Int = 9160) {

  /**
   * the host and port for where this session manager connects.
   */
  val host = Host(url, port, 0)

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
    catch {
      case e:Exception =>
        LoggerFactory.getLogger(getClass).error("No Keyspace: " + keyspace,e)
        KeyspaceDefinition(
          name = keyspace,
          strategyClass = "",
          families = List.empty[FamilyDefinition]
        )
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

  /**
   * Checks to see if the keyspace exists, if it does it will update it, otherwise it wll create it.
   * This is used for testing or development to create a keyspace.
   */
  def loadKeyspace(keyspace: KeyspaceDefinition) {
    val session = new SessionImpl(host, null)
    try {
      session.describeKeySpace(keyspace.name)
      session.updateKeySpace(keyspace)
    }
    catch {
      case e: Exception =>
        session.addKeySpace(keyspace)
    }
  }

  /**
   * Checks to see if the columnFamily exists, if it does it will update it, otherwise it wll create it.
   * This is used for testing or development to create a columnFamily.
   */
  def loadColumnFamily() {

  }

  /**
   * This creates a new session to interact with cassandra using the configured keyspaceDefinition and
   * host.  Using this factory method required you to manage the life cycle your self. I will lazyly
   * open a connect to cassandra but it will not close it.
   *
   * @see SessionImpl
   */
  def createSession:Session = new SessionImpl(host, keyspaceDefinition)

  /**
   * Manaages the life cycle of a session automatically.
   */
  def doWith(f: (Session) => Unit) {
    val session = createSession
    Calista.value = session
    f(session)
    Calista.value = null
    session.close()
  }
}

/**
 * A Host instance for cassandera to connect too.
 */
case class Host(address: String, port: Int, timeout: Int)

