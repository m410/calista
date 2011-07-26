package org.brzy.calista.results

import sun.java2d.SunGraphicsEnvironment.T1Filter

/**
 * A result set for a query on the cassandra data store.
 * 
 * @author Michael Fortin
 */
case class ResultSet(rows:List[Row]) {

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