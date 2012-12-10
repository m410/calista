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

import org.brzy.calista.serializer.Serializers._
import org.brzy.calista.Calista
import org.brzy.calista.results.Row
import org.brzy.calista.serializer.Serializers

/**
 * A key can have one of two parents, a super column or a column family.  This is a standard
 * key which has a column family as a parent.
 * 
 * @author Michael Fortin
 */
class SuperCounterKey[K] protected[schema] (key:K, val family:Family) extends Key{

  def keyBytes = toBytes(key)

  def columnPath = ColumnPath(family.name,null,null)

  def apply[N](columnName: N) =  new SuperCounterColumn(columnName, this)

  def from[N](columnName: N)():SliceRange = {
    def startBytes = Serializers.toBytes(columnName).array()
    new SliceRange(key = this, startBytes = startBytes, start = Option(columnName))
  }

  def to[N](toColumn: N):SliceRange = {
    def bytes = Serializers.toBytes(toColumn).array()
    new SliceRange(key = this, finishBytes = bytes, finish = Option(toColumn))
  }

  /**
   * Used by the DSL to create a SlicePredicate from this key, using this key as the parent.
   */
  def predicate[A](columns:Array[A]) = {
    new SlicePredicate(columns,this)
  }

  /**
   * Removed the super column by this name.
   *
   * @return false if the row does not exist, and true if
   * it's removed successfully.
   */
  def remove() {
    Calista.value.remove(this)
  }

  def list = {
    Calista.value.sliceRange(new SliceRange(key=this,max=100))
  }

  def map[B](f:Row => B):Seq[B] = {
    var seq = collection.mutable.Seq.empty[B]
    val predicate = new SliceRange(key=this,max=2)
    val iterator  = predicate.iterator

    while(iterator.hasNext)
      seq = seq :+ f(iterator.next())

    seq.toSeq
  }


  def foreach(f:Row =>Unit) {
    val slice = new SliceRange(key=this,max=2)
    val iterator  = slice.iterator

    while(iterator.hasNext)
      f(iterator.next())

  }

  override def toString = family + "("+key+")"
}