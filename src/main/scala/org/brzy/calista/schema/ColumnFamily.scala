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

/**
 * Document Me..
 *
 * @author Michael Fortin
 */
case class ColumnFamily(name: String) {
  def |[T](key: T)(implicit t: Manifest[T]) = StandardKey(key, this)

  def |^[T](key: T)(implicit t: Manifest[T]) = SuperKey(key, this)

  def \[T,C](start: T, end: T, columns:List[C], count: Int = 100) =
      KeyRange(start, end, SlicePredicate(columns, null), this, count)
}