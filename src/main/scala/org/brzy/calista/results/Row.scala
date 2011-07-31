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
package org.brzy.calista.results

import java.nio.ByteBuffer
import org.brzy.calista.serializer.Serializers
import java.util.Date

/**
 * A single row in the data store.
 * 
 * @author Michael Fortin
 */
case class Row(
        rowType:RowType,
        family:String,
        key:ByteBuffer,
        superColumn:ByteBuffer,
        column:ByteBuffer,
        value:ByteBuffer,
        timestamp:Date) {

  def keyAs[T:Manifest]:T = as[T](key)

  def superColumnAs[T:Manifest]:T = as[T](column)

  def columnAs[T:Manifest]:T = as[T](superColumn)

  def valueAs[T:Manifest]:T = as[T](value)

  protected[this] def as[T:Manifest](b:ByteBuffer):T =
      Serializers.fromClassBytes(manifest[T].erasure, b.array()).asInstanceOf[T]
}
