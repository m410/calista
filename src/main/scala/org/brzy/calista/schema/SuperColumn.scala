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
import org.brzy.calista.serializer.Serializers
import org.brzy.calista.system.FamilyDefinition
import org.brzy.calista.Calista
import org.brzy.calista.results.Row

/**
 * A super column in a cassandra datastore.
 *
 * @author Michael Fortin
 */
case class SuperColumn[T:Manifest] protected[schema] (name: T, parent: SuperKey[_])
    extends Key{

  def keyBytes = parent.keyBytes

  def nameBytes = Serializers.toBytes(name)

  def family = parent.family

  def columnPath = ColumnPath(parent.family.name, nameBytes,null)

  def apply[C <: Any : Manifest](name: C) = ColumnName(name,this)

//  def counter[N: Manifest](name: N) =  CounterColumnName(name,this)

//  def column[N:Manifest, V:Manifest](sKey: N, value: V, timestamp: Date) = Column(sKey, value, timestamp, this)

//
//  def slicePredicate[A:Manifest](columns:Array[A]) = {
//    SlicePredicate(columns,this)
//  }
//
//
//  def sliceRange[A:Manifest](start:A,end:A,reverse:Boolean,count:Int) = {
//    SliceRange(start,end, reverse, count,this)
//  }

  def column[C:Manifest,V:Manifest](column:C, value:V) = {
    new Column(column,value,new Date(), this)
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
    val slice = new SliceRange(key=this,max=2)
    val iterator  = slice.iterator

    while(iterator.hasNext)
      seq :+ f(iterator.next())

    seq.toSeq
  }

  def foreach(f:Row =>Unit) {
    val slice = new SliceRange(key=this,max=2)
    val iterator  = slice.iterator

    while(iterator.hasNext)
      f(iterator.next())

  }
}