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
import org.brzy.calista.serializer.Serializers
import org.brzy.calista.system.FamilyDefinition

/**
 * A super column in a cassandra datastore.
 *
 * @author Michael Fortin
 */
case class SuperColumn[T:Manifest] protected[schema] (key: T, parent: SuperKey[_],familyDef:FamilyDefinition)
        extends Key {

  def nodePath = parent.nodePath + ":SuperColumn("+key+")"

  def keyBytes = Serializers.toBytes(key)

  def family = parent.family

  def columnPath = ColumnPath(parent.family.name,keyBytes,null)

  override def |[N: Manifest](name: N) = {
    if (familyDef.columnType == "Standard")
      column(name,this)
    else
      counter(name,this)
  }

  def column[N: Manifest](name: N) = ColumnName(name,this)

  def counter[N: Manifest](name: N) =  CounterColumnName(name,this)

  override def ||[N:Manifest, V:Manifest](sKey: N, value: V = null, timestamp: Date = new Date()) =
    column(sKey, value, timestamp)

  def column[N:Manifest, V:Manifest](sKey: N, value: V, timestamp: Date) =
    Column(sKey, value, timestamp, this)

  /**
	 * Used by the DSL to create a SlicePredicate from this key, using this key as the parent.
	 */
  override def \[A:Manifest](columns:A*) = slicePredicate(columns.toArray)

  def slicePredicate[A:Manifest](columns:Array[A]) = SlicePredicate(columns,this)

	/**
	 * Used by the DSL to create a SliceRange from this super column, using this key as the parent.
	 */
  override def \\[A:Manifest](start:A,end:A,reverse:Boolean = false,count:Int = 100) =
    sliceRange(start,end, reverse, count)

  def sliceRange[A:Manifest](start:A,end:A,reverse:Boolean,count:Int) =
    SliceRange(start,end, reverse, count,this)

}