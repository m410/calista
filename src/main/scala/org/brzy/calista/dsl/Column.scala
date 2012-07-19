package org.brzy.calista.dsl

import org.brzy.calista.schema.ColumnFamily

/**
 * Document Me..
 *
 * @author Michael Fortin
 */
object Column {

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
  def apply[K:Manifest,N:Manifest](name:String)(key:K)(columnName:N) = {
    new ColumnFamily(name).standardKey(key).column(columnName)
  }
}
