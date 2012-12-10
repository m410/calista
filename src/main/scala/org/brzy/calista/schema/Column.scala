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

import java.util.Date
import org.brzy.calista.serializer.{Serializers}
import org.brzy.calista.Calista

/**
 * Represents a column in the datastore.  This column should only be created by calling one of the
 * methods of the StandardKey or SuperColumn classes.
 *
 * @param name The name of the column
 * @param value The value of the column
 * @param timestamp The timestamp of the column
 * @param parent The KeyFamily or the SuperColumn parent to this column
 *
 * @author Michael Fortin
 */
class Column[N,V] protected[schema] (val name: N, val value: V, val timestamp: Date, val parent: Key) {
	
	/**
	 * Return the name converted to bytes.
	 */
  def nameBytes = Serializers.toBytes(name) // TODO This doesn't allow for custom serializers

	/**
	 * Return the value converted to bytes.
	 */
  def valueBytes = Serializers.toBytes(value) // TODO This doesn't allow for custom serializers

	/**
	 * Used by the SessionImpl object for querying.  Uses of the column class should not have to use this method
	 * directly.
	 */
  def columnPath = {
    val superCol = parent match {
      case s: SuperColumn[_] => s.nameBytes
      case _ => null
    }
    ColumnPath(parent.family.name, superCol, nameBytes)
  }

	/**
	 * Used by the SessionImpl object for querying.  Uses of the column class should not have to use this method
	 * directly.
	 */
  def columnParent: ColumnParent = parent match {
    case s: StandardKey[_] => ColumnParent(s.family.name, null)
    case s: CounterKey[_] => ColumnParent(s.family.name, null)
    case s: SuperColumn[_] => ColumnParent(s.family.name, s.nameBytes)
    case s: SuperCounterColumn[_] => ColumnParent(s.family.name, s.nameBytes)
  }

  /**
   * Returns a sigle row represented by the column path.
   */
  def row = {
    val session = Calista.value
    session.get(this)
  }

  override def toString = parent.toString + "("+name+","+value+")"
}