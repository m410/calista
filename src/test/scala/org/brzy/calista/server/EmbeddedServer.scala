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
package org.brzy.calista.server

import java.io.{File => JFile}
import org.brzy.fab.file.FileUtils._
import org.apache.thrift.transport.TSocket
import org.apache.cassandra.thrift.CassandraDaemon
import org.apache.cassandra.config.{KSMetaData, CFMetaData, DatabaseDescriptor}
import org.brzy.fab.file.File
import org.slf4j.LoggerFactory
import actors.Actor._
import org.brzy.calista.{SessionManager, Host}
import org.brzy.calista.system.{FamilyDefinition, KeyspaceDefinition}

/**
 * Run an embbeded cassandra server for testing.  Possibly also use for development.
 *
 * @author Michael Fortin
 */
object EmbeddedServer {
  val log = LoggerFactory.getLogger(this.getClass)
  val hosts = Host("localhost", 9160, 250) :: Nil
  val homeDirectory = File("target/cassandra")
  homeDirectory.trash()
  homeDirectory.mkdirs

  log.debug("creating cassandra instance at: " + homeDirectory.getCanonicalPath)
  log.debug("copying cassandra configuration files to root directory")

  val fileSep = System.getProperty("file.separator")

  val storageSource = new JFile(this.getClass.getResource("/cassandra.yaml").getPath)
  storageSource copyTo homeDirectory

  val logSource = new JFile(this.getClass.getResource("/log4j-server.properties").getPath)
  logSource copyTo homeDirectory

  System.setProperty("storage-config", homeDirectory.getCanonicalPath)
  log.debug("created data file and log location directories")

  val daemon = actor {
    val daemon = new CassandraDaemon
    daemon.init(new Array[String](0))
    daemon.start()
  }.start()

  checkConnection()
  loadSchema()

  def checkConnection() {
     // try to make sockets until the server opens up - there has to be a better
  // way - just not sure what it is.
  log.debug("Sleep for 4s")
  Thread.sleep(4000)

  val socket = new TSocket("localhost", 9160)
  var opened = false
  while (!opened) {
    try {
      socket.open()
      opened = true
      log.debug("I was able to make a connection")
    }
    catch {
      case e: Throwable => log.error("******************** Not started", e)
      opened = true
    }
    finally {
      socket.close()
    }
  }
  }


  def loadSchema() {
    log.debug("Setting up the keyspace")
    val mgr = new SessionManager("Test","localhost")
    mgr.doWith({session =>
      session.addKeyspace(KeyspaceDefinition(
        name = "Test",
        strategyClass = "org.apache.cassandra.locator.SimpleStrategy",
        families = List(
          new FamilyDefinition(
            name="Standard",
            comparatorType = Option("UTF8type"))
//          ,
//          new FamilyDefinition(
//            name="StandardFamily",
//            comparatorType = Option("UTF8type")),
//          new FamilyDefinition(
//            name="Person",
//            comparatorType = Option("UTF8type")),
//          new FamilyDefinition(
//            name="SPerson",
//            columnType = "Super",
//            comparatorType = Option("UTF8type"),
//            subcomparatorType = Option("UTF8Type")),
//          new FamilyDefinition(
//            name="Super",
//            columnType = "Super",
//            comparatorType = Option("LongType"),
//            subcomparatorType = Option("LexicalUUIDType")),
//          new FamilyDefinition(
//            name="SuperFamily",
//            columnType = "Super",
//            comparatorType = Option("UTF8type"),
//            subcomparatorType = Option("UTF8Type")),
//          new FamilyDefinition(
//            name="Super2",
//            columnType = "Super",
//            comparatorType = Option("UTF8type"),
//            subcomparatorType = Option("UTF8Type")),
//          new FamilyDefinition(
//            name="CountFamily",
//            columnType = "Counter",
//            comparatorType = Option("UTF8Type"))
        )
      ))
    })
  }
}