package org.brzy.calista.dsl

import org.brzy.calista.schema.ColumnFamily

/**
 * Document Me..
 *
 * @author Michael Fortin
 */
object SuperColumn {

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
  def apply[K:Manifest,S:Manifest,N:Manifest](name:String)(key:K, superKey:S)(columnName:N) = {
    new ColumnFamily(name).superKey(key).superColumn(superKey).column(columnName)
  }
}
