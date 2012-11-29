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
 * Provides a way to create a Super column ColumnName object with a shorter syntax.
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
   * @return a Super Column with name only for a the column family
   */
  def apply[K:Manifest,S:Manifest,N:Manifest](name:String)(key:K)(superKey:S, columnName:N) = {
    new ColumnFamily(name).superKey(key).superColumn(superKey).column(columnName)
  }

  /**
   * Can be used as a simpler syntax to get the key of a column family/
   *
   * @param name the name of the column family
   * @param key the value of the key
   * @tparam K they type of the key
   * @return a Super Column key
   */
  def key[K:Manifest,N:Manifest](name:String)(key:K) = {
    new ColumnFamily(name).superKey(key)
  }

  /**
   * Can be used as a simpler syntax to get the key and super column of a column family/
   *
   * @param name the name of the column family
   * @param key the value of the key
   * @tparam K they type of the key
   * @tparam S the type of the super column name
   * @return a Super Column
   */
  def superCol[K:Manifest,S:Manifest](name:String)(key:K)(superKey:S) = {
    new ColumnFamily(name).superKey(key).superColumn(superKey)
  }
}
