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

import org.brzy.calista.schema.{SliceRange, ColumnName, ColumnFamily}
import java.util.Iterator
import org.brzy.calista.results.Row
import org.brzy.calista.{Session, Calista}

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

//  implicit def <=[K,V](column:ColumnName[K])(implicit mk:Manifest[K]) = {
//    val session = Calista.value.get
//      session.get(column)
//  }

  class ColumnOps[K:Manifest](column:ColumnName[K]) {
    def <=[T:Manifest](t:T) {
      column.set(t)
    }
  }

  implicit def columnOps[K:Manifest](column:ColumnName[K]) = {
    new ColumnOps(column)
  }

}