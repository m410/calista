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
package org.brzy.calista.schema

import org.brzy.calista.Calista
import org.brzy.calista.serializer.Serializers._


/**
 * Represents a column family to query in the database.  This is the entry point to DSL access
 * to the datastore.
 *
 * @author Michael Fortin
 */
trait Family {

  def name:String

  def apply[K](key: K):Key

  def to[K](key: K) = {
    def keyBytes = toBytes(key).array()
    new KeyRange(finish = Option(key), finishBytes = keyBytes, columnFamily = this)
  }

  def from[K](key: K) = {
    def keyBytes = toBytes(key).array()
    new KeyRange(start = Option(key), startBytes = keyBytes, columnFamily =  this)
  }

  def definition = try {
    Calista.value.ksDef.families.find(_.name == name) match {
      case Some(f) => f
      case _ => throw new UnknownFamilyException("No ColumnFamily with name: " + name)
    }
  }
  catch {
    case n:NullPointerException => throw new SessionScopeException("Out of session scope",n)
  }

  override def toString = "ColumnFamily("+name+")"

}
