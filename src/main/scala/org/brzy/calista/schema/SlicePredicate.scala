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

import org.brzy.calista.serializer.Serializers
import org.brzy.calista.Calista

/**
 * Used to query the datastore, and only return columns within the provided list.
 *
 * @tparam T The type of the colums
 * @param columns The column names that are to be returned.
 * @param key The parent key to the columns that are to be return.
 *
 * @author Michael Fortin
 */
class SlicePredicate[T] protected[schema](val columns: Array[T], val key: Key) {

  def toByteList = columns.map(c => Serializers.toBytes(c)).toList

  def columnParent: ColumnParent = key match {
    case s: StandardKey[_] => ColumnParent(s.family.name, null)
    case s: CounterKey[_] => ColumnParent(s.family.name, null)
    case s: SuperColumn[_] => ColumnParent(s.family.name, s.nameBytes)
    case s: SuperCounterColumn[_] => ColumnParent(s.family.name, s.nameBytes)
  }

  def results = {
    val session = Calista.value
    session.slice(this)
  }
}
