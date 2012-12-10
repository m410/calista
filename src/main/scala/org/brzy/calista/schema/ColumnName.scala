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
import java.util.Date
import org.brzy.calista.serializer.Serializers


/**
 * Represents only the name part of a column, not the value or timestamp.  It's used to query
 * the datastore.
 * 
 * @author Michael Fortin
 */
class ColumnName protected[schema] (val name:Any, val parent:Key) {

  /**
   * Return the name converted to bytes.
   */
  def nameBytes = Serializers.toBytes(name)

  def asColumn = new Column(name,null,null, parent)

  /**
   * Used by the SessionImpl object for querying.  Uses of the column class should not have to use this method
   * directly.
   */
  def columnPath = {
    val superCol = parent match {
      case s: SuperColumn => s.keyBytes
      case _ => null
    }
    ColumnPath(parent.family.name, superCol, nameBytes)
  }

  /**
   * Used by the SessionImpl object for querying.  Uses of the column class should not have to use this method
   * directly.
   */
  def columnParent: ColumnParent = parent match {
    case s: StandardKey => ColumnParent(s.family.name, null)
    case s: SuperColumn => ColumnParent(s.family.name, s.keyBytes)
  }

  /**
   * Set the value of the column and insert it immediately.
   */
  def set[V <: Any : Manifest](value: V) {
    Calista.value.insert(new Column(name, value, new Date(), parent))
  }

  def columnExists:Boolean = {
    Calista.value.get(new Column(name, null, new Date(), parent)) match {
      case Some(row) => true
      case _ => false
    }
  }

  /**
   * Get the value of the column
   *
   * @tparam V The expected type of the column value
   * @return Optional value, if the column by that name exists, it returns Some(V) otherwise none.
   */
  def getAs[V<:Any: Manifest] = {
    Calista.value.get(new Column(name, null, new Date(), parent)) match {
      case Some(row) => Option(row.valueAs[V])
      case _ => None
    }
  }

  /**
   * Removed the column by this name.
   *
   * @return false if the row does not exist, and true if
   * it's removed successfully.
   */
  def remove() {
    Calista.value.remove(this)
  }


}