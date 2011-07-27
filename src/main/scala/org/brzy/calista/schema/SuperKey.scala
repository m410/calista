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
import org.brzy.calista.FamilyDefinition

/**
 * A super key has a Column family as a parent.
 * 
 * @author Michael Fortin
 */
protected case class SuperKey[T:Manifest](key:T,family:ColumnFamily, familyDef:FamilyDefinition)
        extends Key
        with DslNode {

  def nodePath = family.nodePath + ":SuperKey("+key+")"

  def keyBytes = Serializers.toBytes(key)

  def columnPath = ColumnPath(family.name,keyBytes,null)

  override def |[N:Manifest](sKey:N) = superColumn(sKey,this)

  def superColumn[N:Manifest](sKey:N) = SuperColumn(sKey,this,familyDef)

  /**
	 * Used by the DSL to create a SliceRange from this super column, using this key as the parent.
	 */
  override def \\[A:Manifest](start:A,end:A,reverse:Boolean = false,count:Int = 100) =
    sliceRange(start, end, reverse, count).resultSet

  def sliceRange[A:Manifest](start:A,end:A,reverse:Boolean,count:Int) =
    SliceRange(start, end, reverse, count,this)

}