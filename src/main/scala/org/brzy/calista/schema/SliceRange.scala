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
 * @param start The first entry to return.
 * @param finish The last entry to return.
 * @param reverseList Reverse the order, default to false.
 * @param max The max number of results
 * @param key The key parent to the columns that are being sliced.
 *
 * @author Michael Fortin
 */
class SliceRange protected[schema](
        val key: Key,
        val startBytes: Array[Byte] = Array.empty[Byte],
        val start: Option[Any] = None,
        val finishBytes: Array[Byte] = Array.empty[Byte],
        val finish: Option[Any] = None,
        val reverseList: Boolean = false,
        val max: Int = 100
        ) {


  def keyBytes = key.keyBytes

  def copy(start: Array[Byte], finish: Array[Byte]) = {
    new SliceRange(key, start, None, finish, None, reverseList, max)
  }

  def from[T <: Any : Manifest](startKey: T)(): SliceRange = {
    def startKeyBytes = Serializers.toBytes(startKey).array()
    new SliceRange(key, startKeyBytes, Option(startKey), finishBytes, finish, reverseList, max)
  }


  def to[T <: Any : Manifest](finishCol: T): SliceRange = {
    def finishColBytes = Serializers.toBytes(finishCol).array()
    new SliceRange(key, startBytes, start, finishColBytes, Option(finishCol), reverseList, max)
  }

  def reverse(go:Boolean) = {
    new SliceRange(key, startBytes, start, finishBytes, finish, go, max)
  }

  def limit(maxResults: Int) = {
    new SliceRange(key, startBytes, start, finishBytes, finish, reverseList, maxResults)
  }

  def columnParent: ColumnParent = key match {
    case s: StandardKey[_] => ColumnParent(s.family.name, null)
    case s: CounterKey[_] => ColumnParent(s.family.name, null)
    case s: SuperKey[_] => ColumnParent(s.family.name, null)
    case s: SuperCounterKey[_] => ColumnParent(s.family.name, null)
    case s: SuperColumn[_] => ColumnParent(s.family.name, s.nameBytes)
    case s: SuperCounterColumn[_] => ColumnParent(s.family.name, s.nameBytes)
  }

  def foreach(f: Row => Unit) {
    val iterator = this.iterator

    while (iterator.hasNext)
      f(iterator.next())

  }

  def map[B](f: Row => B): Seq[B] = {
    var seq = collection.mutable.Seq.empty[B]
    val iterator = this.iterator

    while (iterator.hasNext)
      seq :+ f(iterator.next())

    seq.toSeq
  }

  def results = {
    val session = Calista.value
    session.sliceRange(this)
  }

  def iterator: Iterator[Row] = {
    val session = Calista.value
    session.scrollSliceRange(this)
  }

  override def toString = key + "(" + start + ":" + finish + ")"
}