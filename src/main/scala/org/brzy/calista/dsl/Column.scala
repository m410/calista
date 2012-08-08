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
package org.brzy.calista.dsl

import org.brzy.calista.schema.ColumnFamily

/**
 * Provides a way to create a ColumnName object with a shorter syntax.
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

  /**
   * Can be used as a simpler syntax to get the key of a column family/
   *
   * @param name the name of the column family
   * @param key the value of the key
   * @tparam K they type of the key
   * @return a Column with name only for a standard column family
   */
  def key[K:Manifest,N:Manifest](name:String)(key:K) = {
    new ColumnFamily(name).standardKey(key)
  }
}
