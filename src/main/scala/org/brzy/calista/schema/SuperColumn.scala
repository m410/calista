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

/**
 * A super column in a cassandra datastore.
 *
 * @author Michael Fortin
 */
protected case class SuperColumn[T](key: T, parent: SuperKey[_])(implicit m: Manifest[T])
        extends Key with ColumnOrSuperColumn {

	/**
	 * Used by the DSL to create a Column from this key, using this super column as the parent.
	*/
  def |[N, V](sKey: N, value: V = null, timestamp: Date = new Date())(implicit n: Manifest[N], v: Manifest[V]) =
    Column(sKey, value, timestamp, this)

  /**
	 * Used by the DSL to create a SlicePredicate from this key, using this key as the parent.
	 */
  def \\[A<:AnyRef](columns:List[A]) = SlicePredicate(columns,this)

	/**
	 * Used by the DSL to create a SliceRange from this super column, using this key as the parent.
	 */
  def \[A](start:A,end:A,count:Int = 100) = SliceRange(start,end,true, count,this)

  def keyBytes = Serializers.toBytes(key)

  def family = parent.family

  def columnPath = ColumnPath(parent.family.name,keyBytes,null)
}