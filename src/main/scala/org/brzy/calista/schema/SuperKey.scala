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
import org.brzy.calista.system.FamilyDefinition
import org.brzy.calista.Calista
import org.brzy.calista.results.Row

/**
 * A super key has a Column family as a parent.
 *
 * @author Michael Fortin
 */
class SuperKey[K] protected[schema](val key: K, val family: Family) extends Key {

  def keyBytes = Serializers.toBytes(key)

  def columnPath = ColumnPath(family.name, null, null)

  def apply[N](sKey: N) = new SuperColumn(sKey, this)


  def list = {
    Calista.value.sliceRange(new SliceRange(key = this, max = 100))
  }

  def map[B](f: Row => B): Seq[B] = {
    var seq = collection.mutable.Seq.empty[B]
    val slice = new SliceRange(key = this, max = 2)
    val iterator = slice.iterator

    while (iterator.hasNext)
      seq = seq :+ f(iterator.next())

    seq.toSeq
  }


  def foreach(f: Row => Unit) {
    val slice = new SliceRange(key = this, max = 2)
    val iterator = slice.iterator

    while (iterator.hasNext)
      f(iterator.next())

  }

  override def toString = family + "(" + key + ")"

}