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
import org.brzy.calista.Calista
import org.brzy.calista.serializer.Serializers

/**
 * This represents the counter column when querying cassandra
 *
 * @author Michael Fortin
 */
class CounterColumnName[N] protected[schema](val name: N, val parent: Key) {

  /**
   * Return the name converted to bytes.
   */
  def nameBytes = Serializers.toBytes(name)

  def asColumn = new Column(name, null, null, parent)

  /**
   * Used by the SessionImpl object for querying.  Uses of the column class should not have to use this method
   * directly.
   */
  def columnPath = {
    val superCol = parent match {
      case s: SuperColumn[_] => s.keyBytes
      case s: SuperCounterColumn[_] => s.keyBytes
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
    case s: SuperColumn[_] => ColumnParent(s.family.name, s.keyBytes)
  }

  def -=(amount: Long) {
    add(-amount)
  }

  def +=(amount: Long) {
    add(amount)
  }

  def add(amount: Long) {
    val session = Calista.value
    session.add(new Column(name, amount, new Date(), parent))
  }

  def count = {
    val session = Calista.value
    session.get(new Column(name, null, new Date(), parent)) match {
      case Some(c) => c.valueAs[Long]
      case _ => throw new NoColumnException("No column for name: " + name)
    }
  }

  def remove() {
    Calista.value.remove(this)
  }
}