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
package org.brzy.calista.schema

import org.brzy.calista.ocm.Calista
import org.brzy.calista.Session

/**
 * Represents a column family to query in the database.
 *
 * @param name the name of the column family.
 * @author Michael Fortin
 */
case class ColumnFamily(name: String) {
	
	/**
	 * Add a child key to the column family, return that key.  
	 */
  def |[T](key: T)(implicit t: Manifest[T]) = StandardKey(key, this)

	/**
	 * Add a child super column to the column family, return that super column.  
	 */
  def |^[T](key: T)(implicit t: Manifest[T]) = SuperKey(key, this)

  def key[T](key: T)(implicit t: Manifest[T]) = {
    val family = Calista.value.asInstanceOf[Session].ksDef.families.find(_.name == name).get

    if(family.columnType == "Standard")
      StandardKey(key, this)
    else
      SuperKey(key,this)
  }

	/**
	 * Create a key range from this key family used to query the datastore.
	 */
  def \[T,C](start: T, end: T, columns:List[C], count: Int = 100) =
			KeyRange(start, end, SlicePredicate(columns, null), this, count)
}