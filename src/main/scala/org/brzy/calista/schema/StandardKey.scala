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
import org.brzy.calista.serializer.Serializers._

/**
 * A key can have one of two parents, a super column or a column family.  This is a standard
 * key which has a column family as a parent.
 * 
 * @author Michael Fortin
 */
case class StandardKey[T](key:T, family:ColumnFamily)(implicit m:Manifest[T]) extends Key {

  def keyBytes = {
    val buf = toBytes(key)
    buf
  }

  def columnPath = ColumnPath(family.name,null,null)

	/**
	 * Used by the DSL to create a column from this key, and setting this key as the columns parent.
	 */
  def |[N,V](key:N,value:V = null,timestamp:Date = new Date())(implicit n:Manifest[N],v:Manifest[V]) =
    Column(key,value,timestamp,this)

	/**
	 * Used by the DSL to create a SlicePredicate from this key, using this key as the parent.
	 */
  def \\[A<:AnyRef](columns:List[A]) = SlicePredicate(columns,this)
  
	/**
	 * Used by the DSL to create a SliceRange from this key, using this key as the parent.
	 */
  def \[A](start:A,end:A,count:Int = 100) = SliceRange(start,end,true, count,this)
}