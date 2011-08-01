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
package org.brzy.calista.results

import collection.mutable.ListBuffer

/**
 * A result set for a query on the cassandra data store.
 * 
 * @author Michael Fortin
 */
case class ResultSet(rows:List[Row]) {

  def size = rows.size

  def isEmpty = rows.isEmpty
  
  /**
   * Converts the rows returned by the query into a map where the key is the column name.
   */
  def toColumnMap[T:Manifest]:Map[T,Row] = rows.map(r=>r.columnAs[T] -> r).toMap

  /**
   * converts the returned rows to a map where the key in the datastore is the key of the map.
   */
  def toKeyMap[T:Manifest]:Map[T,List[Row]] = {
    val map = rows.map(r=>r.keyAs[T] -> ListBuffer.empty[Row]).toMap
    rows.foreach(r=>map(r.keyAs[T]) += r)
    map.map(m=> m._1 -> m._2.toList)
  }

  def toKeySuperMap[K:Manifest,S:Manifest]:Map[(K,S),List[Row]] = {
    val map = rows.map(r=> (r.keyAs[K]->r.superColumnAs[S]) -> ListBuffer.empty[Row]).toMap
    rows.foreach(r=>map((r.keyAs[K]->r.superColumnAs[S])) += r)
    map.map(m=> m._1 -> m._2.toList)
  }

  /**
   * Converts the rows to arrays of the supplied data types.  This applies to standard columns
   * only and the values have to be of the same type.
   */
  def toArrays[K:Manifest,N:Manifest,V:Manifest] =
      rows.map(r=> Array(r.keyAs[K],r.columnAs[N],r.valueAs[V]))

  /**
   * Converts the rows to arrays of the supplied data types.  This applies to super columns
   * only and the values have to be of the same data type.
   */
  def toSuperArrays[K:Manifest,S:Manifest,N:Manifest,V:Manifest] =
      rows.map(r=> Array(r.keyAs[K],r.superColumnAs[S],r.columnAs[N],r.valueAs[V]))
}