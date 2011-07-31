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

/**
 * A super key has a Column family as a parent.
 * 
 * @author Michael Fortin
 */
case class SuperKey[T:Manifest] protected[schema] (key:T,family:ColumnFamily, familyDef:FamilyDefinition)
    extends Key {

  def keyBytes = Serializers.toBytes(key)

  def columnPath = ColumnPath(family.name,keyBytes,null)

  def |[N:Manifest](sKey:N) = superColumn(sKey)

  def superColumn[N:Manifest](sKey:N) = SuperColumn(sKey,this,familyDef)

  /**
	 * Used by the DSL to create a SliceRange from this super column, using this key as the parent.
	 */
  def \\[A:Manifest](start:A,end:A,reverse:Boolean = false,count:Int = 100) =
    sliceRange(start, end, reverse, count)

  def sliceRange[A:Manifest](start:A,end:A,reverse:Boolean,count:Int) =
    SliceRange(start, end, reverse, count,this)

	/**
	 * Used by the DSL to create a SlicePredicate from this key, using this key as the parent.
	 */
  def \[A:Manifest](columns:A*) = predicate(columns.toArray)

  def predicate[A:Manifest](columns:Array[A]) = SlicePredicate(columns,this)
}