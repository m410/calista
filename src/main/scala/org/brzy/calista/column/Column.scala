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
package org.brzy.calista.column

import java.nio.ByteBuffer
import java.util.Date
import org.brzy.calista.schema.{Types, Utf8Type}

/**
 * Document Me..
 *
 * @author Michael Fortin
 */
case class Column[K, V](name: K, value: V, timestamp: Date, parent: Key)(implicit k: Manifest[K], v: Manifest[V])
        extends ColumnOrSuperColumn {
  def nameBytes = Types.toBytes(name)

  def valueBytes = Types.toBytes(value)

  def columnPath = {
    val superCol = parent match {
      case s: SuperColumn[_] => s.keyBytes
      case _ => null
    }
    ColumnPath(parent.family.name, superCol, nameBytes)
  }

  def columnParent: ColumnParent = parent match {
    case s: StandardKey[_] => ColumnParent(s.family.name, null)
    case s: SuperColumn[_] => ColumnParent(s.family.name, s.keyBytes)
//    case s: SuperColumn[_] => ColumnParent(s.family.name, s.superKey.keyBytes)
  }
}