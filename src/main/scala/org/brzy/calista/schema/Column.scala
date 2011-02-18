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

import java.nio.ByteBuffer
import java.util.Date
import org.brzy.calista.serializer.{Serializers, UTF8Serializer}

/**
 * Represents a column in the datastore.  
 *
 * @tparam K The key or name column type
 * @tparam V The value type
 * @param name The name of the column
 * @param value The value of the column
 * @param timestamp The timestamp of the column
 * @param parent The KeyFamily or the SuperColumn parent to this column
 *
 * @author Michael Fortin
 */
case class Column[K, V](name: K, value: V, timestamp: Date, parent: Key)(implicit k: Manifest[K], v: Manifest[V])
        extends ColumnOrSuperColumn {
	
	/**
	 * Return the name converted to bytes.
	 */
  def nameBytes = Serializers.toBytes(name)

	/**
	 * Return the value converted to bytes.
	 */
  def valueBytes = Serializers.toBytes(value)

	/**
	 * Used by the Session object for querying.  Uses of the column class should not have to use this method
	 * directly.
	 */
  def columnPath = {
    val superCol = parent match {
      case s: SuperColumn[_] => s.keyBytes
      case _ => null
    }
    ColumnPath(parent.family.name, superCol, nameBytes)
  }

	/**
	 * Used by the Session object for querying.  Uses of the column class should not have to use this method
	 * directly.
	 */
  def columnParent: ColumnParent = parent match {
    case s: StandardKey[_] => ColumnParent(s.family.name, null)
    case s: SuperColumn[_] => ColumnParent(s.family.name, s.keyBytes)
  }
}