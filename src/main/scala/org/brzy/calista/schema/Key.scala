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

import java.nio.ByteBuffer
import org.brzy.calista.serializer.Serializers._
import org.brzy.calista.Calista

/**
 * Common traits to keys and super columns.  
 * 
 * @author Michael Fortin
 */
trait Key {

  def keyBytes:ByteBuffer

  def family:Family

  def columnPath:ColumnPath

  def from[N](columnName: N)():SliceRange = {
    def startBytes = toBytes(columnName).array()
    new SliceRange(key = this, startBytes = startBytes, start = Option(columnName))
  }

  def to[N](toColumn: N):SliceRange = {
    def bytes = toBytes(toColumn).array()
    new SliceRange(key = this, finishBytes = bytes, finish= Option(toColumn))
  }

  /**
   * Used by the DSL to create a SlicePredicate from this key, using this key as the parent.
   */
  def predicate[A](columns:Array[A]) = new SlicePredicate(columns, this)


  /**
   * Removed the super column by this name.
   *
   * @return false if the row does not exist, and true if
   * it's removed successfully.
   */
  def remove() {
    Calista.value.remove(this)
  }
}