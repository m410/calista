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
package org.brzy.calista

import schema.Consistency._
import results.{Row, ResultSet}
import schema._
import schema.ColumnName
import schema.SlicePredicate
import schema.SliceRange
import schema.SuperKey
import system.{FamilyDefinition, KeyspaceDefinition}
import java.nio.ByteBuffer

/**
 * The session interface for interacting with cassandra.  It's a trait so that it can be mocked
 * for testing.
 *
 * @author Michael Fortin
 */
trait Session {

  def ksDef: KeyspaceDefinition

  def defaultConsistency: Consistency

  /**
   * socket connections are lazily and implicitly opened, but must be explicitly closed.  To end
   * a session this must be called.
   */
  def close()

  def closeAndMakeNewSession: Session

  /**
   * get the value of the column.  This assumes the input column does not have a value, this will
   * return a results.Column with the name and value
   */
  def get(column: Column[_, _]): Option[Row]

  /**
   * Increments a counter column.
   */
  def add(column: Column[_, _], level: Consistency = defaultConsistency)

  /**
   * Read the value of a single column, with the given consistency.
   *
   * @return An Option ColumnOrSuperColumn on success or None
   */
  def get(column: Column[_, _], level: Consistency): Option[Row]

  /**
   * Set the value on an single Column
   */
  def insert(column: Column[_, _], level: Consistency = defaultConsistency)

  /**
   * Remove a column and it's value.
   */
  def remove(column: ColumnName[_])

  /**
   * Remove a counter column and it's value.
   */
  def remove(column: CounterColumnName[_])

  /**
   * Remove a counter column and it's value.
   */
  def remove(column: SuperColumn[_])

  /**
   * Remove a column and it's value.
   */
  def remove(column: Column[_, _])

  /**
   * Remove a key and all it's child columns by using the default consistency level.
   */
  def remove(key: Key)

  def remove(key: SuperCounterColumn[_])

  /**
   * Removes a key by the path and timestamp with the given consistency level.
   */
  def remove(k: ByteBuffer, path: ColumnPath, timestamp: Long, level: Consistency)

  /**
   * get the count of the number of columns for a key
   */
  def count(key: Key, level: Consistency = defaultConsistency): Long

  /**
   * List the columns by slice predicate.  This uses the default consistency.
   */
  def slice(predicate: SlicePredicate[_]): ResultSet

  /**
   * List all the columns by slice predicate and consistency level.
   */
  def slice(predicate: SlicePredicate[_], level: Consistency): ResultSet

  /**
   * List all the columns by slice range. This uses the default consistency.
   */
  def sliceRange(range: SliceRange): ResultSet

  /**
   * List all the columns by slice range and Consistency Level. This uses the default consistency.
   */
  def sliceRange(range: SliceRange, level: Consistency): ResultSet

  /**
   * Scroll through large slices by grabbing them form the datastore in chunks.  This will
   * iterate over all elements including the start and end columns.
   *
   * @param initSliceRange the slice range to iterate over.
   */
  def scrollSliceRange(initSliceRange: SliceRange): Iterator[Row]

  /**
   * Queries the data store by returning the key range inclusively.
   */
  def keyRange(range: KeyRange[_], level: Consistency = defaultConsistency): ResultSet

  /**
   * List all the columns by Key range using the default consistency.
   */
  def keyRange[T <: AnyRef](range: KeyRange[T]): ResultSet

  def query(query: String, compression: String = ""): ResultSet

  def addKeySpace(ks: KeyspaceDefinition)

  def updateKeySpace(keySpace: KeyspaceDefinition)

  def dropKeySpace(keySpace: String)

  def addColumnFamily(family: FamilyDefinition)

  def updateColumnFamily(family: FamilyDefinition)


  def dropColumnFamily(family: String)

  def describeKeySpace(name: String): KeyspaceDefinition

  def describeKeySpaces: List[KeyspaceDefinition]

  def describeClusterName: String

  def describeVersion: String

}
