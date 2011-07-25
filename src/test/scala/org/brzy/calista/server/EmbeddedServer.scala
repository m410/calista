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
import org.brzy.calista.Host
import org.apache.cassandra.thrift.CassandraDaemon
import org.apache.cassandra.config.{KSMetaData, CFMetaData, DatabaseDescriptor}
import org.brzy.fab.file.File
import org.slf4j.LoggerFactory
import actors.Actor._

/**
 * Document Me..
 *
 * @author Michael Fortin
 */
object EmbeddedServer {
  val log = LoggerFactory.getLogger(this.getClass)
  val hosts = Host("localhost", 9160, 250) :: Nil
  val homeDirectory = File("target/cassandra.home.unit-tests")
  homeDirectory.trash()
  homeDirectory.mkdirs

  log.debug("creating cassandra instance at: " + homeDirectory.getCanonicalPath)
  log.debug("copying cassandra configuration files to root directory")

  val fileSep = System.getProperty("file.separator")

  val storageSource = new JFile(this.getClass.getResource("/cassandra.yaml").getPath)
  storageSource copyTo homeDirectory

  val logSource = new JFile(this.getClass.getResource("/log4j-server.properties").getPath)
  logSource copyTo homeDirectory

//  loadSchemaFromYaml
  System.setProperty("storage-config", homeDirectory.getCanonicalPath)
  log.debug("creating data file and log location directories")

  val daemon = actor {
    val daemon = new CassandraDaemon
    daemon.init(new Array[String](0))
    daemon.start()
  }.start()

  // try to make sockets until the server opens up - there has to be a better
  // way - just not sure what it is.
  Thread.sleep(3000)
  log.debug("Sleep for 3s")

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

//  def loadSchemaFromYaml = {
//    import collection.JavaConversions._
//
//    for (ksm: KSMetaData <- DatabaseDescriptor.readTablesFromYaml()) {
//      for (cfm: CFMetaData <- ksm.cfMetaData().values())
//        CFMetaData.map(cfm)
//      DatabaseDescriptor.setTableDefinition(ksm, DatabaseDescriptor.getDefsVersion)
//    }
//  }
}