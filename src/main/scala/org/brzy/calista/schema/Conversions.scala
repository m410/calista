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

/**
 * This needs to be imported so that DSL to create columns can be used.
 * {{{
 * import Conversions._
 * val column = "family" | "key" | ("name","value")
 *}}}
 * 
 * @author Michael Fortin
 */
object Conversions {
  implicit def toKeyFamily(str:String) = ColumnFamily(str)

  class ColumnOps[K,V](column:Column[K,V])(implicit mk:Manifest[K], mv:Manifest[V]) {
    def <=[T](t:T)(implicit m:Manifest[T]) = {
      val session = Calista.value.get
      session.insert(column.copy(value=t))
    }
  }

  implicit def <=[K,V](column:Column[K,V])(implicit mk:Manifest[K], mv:Manifest[V]) = {
    val session = Calista.value.get
      session.get(column)
  }

  implicit def columnOps[K,V](column:Column[K,V])(implicit mk:Manifest[K], mv:Manifest[V]) = {
    new ColumnOps(column)
  }
}