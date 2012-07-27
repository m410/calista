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
  def remove(key: StandardKey[_])

  def remove(key: SuperKey[_])

  /**
   * Removes a key by the path and timestamp with the given consistency level.
   */
  def remove(k: ByteBuffer, path: ColumnPath, timestamp: Long, level: Consistency)

  /**
   * get the count of the number of columns for a key
   */
  def count(key: Key, level: Consistency = defaultConsistency): Long

  /**
   * List all the columns under the given key.
   */
  def list(key: StandardKey[_]): ResultSet

  /**
   * List all the columns under the given super column.
   */
  def list(sc: SuperColumn[_]): ResultSet

  /**
   * List all the super columns and columns under the key
   */
  def list(sc: SuperKey[_]): ResultSet

  /**
   * List all the super columns and columns under the key
   */
  def list[T:Manifest](cn: ColumnName[T]): ResultSet

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
  def sliceRange(range: SliceRange[_]): ResultSet

  /**
   * List all the columns by slice range and Consistency Level. This uses the default consistency.
   */
  def sliceRange(range: SliceRange[_], level: Consistency): ResultSet

  /**
   *  Scroll through large slices by grabbing them form the datastore in chunks.  This will
   *  iterate over all elements including the start and end columns.
   *
   *  @param initSliceRange the slice range to iterate over.
   */
  def scrollSliceRange[T:Manifest](initSliceRange: SliceRange[T]):Iterator[Row]

  /**
   * Queries the data store by returning the key range inclusively.
   */
  def keyRange(range: KeyRange[_,_], level: Consistency = defaultConsistency): ResultSet
  /**
   * List all the columns by Key range using the default consistency.
   */
  def keyRange[T <: AnyRef, C <: AnyRef](range: KeyRange[T, C]): ResultSet

  def query(query:String,compression:String = ""):ResultSet

  def addKeyspace(ks:KeyspaceDefinition)

  def updateKeyspace(keyspace:KeyspaceDefinition)

  def dropKeyspace(keyspace:String)

  def addColumnFamily(family:FamilyDefinition)

  def updateColumnFamily(family:FamilyDefinition)


  def dropColumnFamily(family:String)

  def describeKeyspace(name:String):KeyspaceDefinition

  def describeKeyspaces:List[KeyspaceDefinition]

  def describeClusterName:String

  def describeVersion:String

}
