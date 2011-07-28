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

import sun.java2d.SunGraphicsEnvironment.T1Filter

/**
 * A result set for a query on the cassandra data store.
 * 
 * @author Michael Fortin
 */
case class ResultSet(rows:List[Row]) {

  def size = rows.size
  
  /**
   * Converts the rows returned by the query into a map where the key is the column name.
   */
  def asColumn[T:Manifest]:Map[T,Row] = rows.map(r=>r.columnAs[T] -> r).toMap

  /**
   * converts the returned rows to a map where the key in the datastore is the key of the map.
   */
  def asKeyMap[T:Manifest]:Map[T,Row] = rows.map(r=>r.keyAs[T] -> r).toMap

  /**
   * Converts the rows to arrays of the supplied data types.  This applies to standard columns
   * only and the values have to be of the same type.
   */
  def asColumnArrays[K:Manifest,N:Manifest,V:Manifest] =
      rows.map(r=> Array(r.keyAs[K],r.columnAs[N],r.valueAs[V]))

  /**
   * Converts the rows to arrays of the supplied data types.  This applies to super columns
   * only and the values have to be of the same data type.
   */
  def asSuperColumnArrays[K:Manifest,S:Manifest,N:Manifest,V:Manifest] =
      rows.map(r=> Array(r.keyAs[K],r.superColumnAs[S],r.columnAs[N],r.valueAs[V]))
}