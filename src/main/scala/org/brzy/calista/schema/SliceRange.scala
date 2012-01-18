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
import org.brzy.calista.results.Row
import collection.Iterator

/**
 * Slice of columns under a key.
 *
 * @tparam T they type of the column name.
 * @param start The first entry to return.
 * @param finish The last entry to return.
 * @param reverse Reverse the order, default to false.
 * @param max The max number of results
 * @param key The key parent to the columns that are being sliced.
 *
 * @author Michael Fortin
 */
case class SliceRange[T:Manifest] protected[schema] (start: T, finish: T, reverse: Boolean, max: Int, key: Key){

  def startBytes = Serializers.toBytes(start)

  def finishBytes = Serializers.toBytes(finish)

	def keyBytes = key.keyBytes

  def columnParent: ColumnParent = key match {
    case s: StandardKey[_] => ColumnParent(s.family.name, null)
    case s: SuperKey[_] => ColumnParent(s.family.name, null)
    case s: SuperColumn[_] => ColumnParent(s.family.name, s.nameBytes)
  }

  def results = {
    val session = Calista.value
    session.sliceRange(this)
  }

  def iterator:Iterator[Row] = {
    val session = Calista.value
    session.scrollSliceRange(this)
  }
}