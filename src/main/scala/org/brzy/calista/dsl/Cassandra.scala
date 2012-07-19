package org.brzy.calista.dsl

import org.brzy.calista.schema.ColumnFamily

/**
 * This provides some helpers to create columns with a simpler syntax.  For exmaple:
 *
 * {{{
 *   import dsl.Cassandra._
 *   def something() {
 *     val stdColName = columnName("StandardFamily")("key")("columnName")
 *     val columnValue = stdColName.valueAs[String]
 *   }
 * }}}
 *
 * @author Michael Fortin
 */
object Cassandra {

  /**
   * Created a standard column by family, key and name, with no value
   *
   * @param name the name of the column family
   * @param key the value of the key
   * @param columnName the name of the column
   * @tparam K they type of the key
   * @tparam N they type of the column name
   * @return a Column with name only for a standard column family
   */
  def columnName[K:Manifest,N:Manifest](name:String)(key:K)(columnName:N) = {
    new ColumnFamily(name).standardKey(key).column(columnName)
  }

  def column[K:Manifest,N:Manifest,V:Manifest](name:String)(key:K)(columnName:N, columnValue:V) = {
    new ColumnFamily(name).standardKey(key).column(columnName,columnValue)
  }

  /**
   *  Create a key with column family
   * @param name the name of the family
   * @param key the key
   * @tparam K the type of the key
   * @return a standard column family key
   */
  def columnKey[K:Manifest](name:String)(key:K) = {
    new ColumnFamily(name).standardKey(key)
  }

  /**
   * Creates a super column, column with name and no value.
   *
   * @param name the name of the column family
   * @param key the key
   * @param superKey the super column name
   * @param columnName the name of the column
   * @tparam K the type of the key
   * @tparam S the type of the super column name
   * @tparam N the type of the name of the column
   * @return a Column with name only for a super column family
   */
  def superColumnName[K:Manifest,S:Manifest,N:Manifest](name:String)(key:K, superKey:S)(columnName:N) = {
    new ColumnFamily(name).superKey(key).superColumn(superKey).column(columnName)
  }

  def superColumn[K:Manifest,S:Manifest,N:Manifest,V:Manifest](name:String)(key:K, superKey:S)(columnName:N, columnValue:V) = {
    new ColumnFamily(name).superKey(key).superColumn(superKey).column(columnName,columnValue)
  }


  /**
   *  Create a super column key and super column.
   *
   * @param name The name of the column family
   * @param key the key
   * @param superKey the super key
   * @tparam K the key type
   * @tparam S the super key type
   * @return a super column name for the column family.
   */
  def superColumnKey[K:Manifest,S:Manifest](name:String)(key:K, superKey:S) = {
    new ColumnFamily(name).superKey(key).superColumn(superKey)
  }
}
