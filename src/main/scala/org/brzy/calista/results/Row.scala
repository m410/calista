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
import RowType._
import org.apache.commons.lang.builder.ToStringBuilder

/**
 * A single row in the data store.
 *
 * @author Michael Fortin
 */
class Row(
        _rowType: RowType,
        _family: String,
        _key: ByteBuffer,
        _superColumn: ByteBuffer,
        _column: ByteBuffer,
        _value: ByteBuffer,
        _timestamp: Date) {

  def rowType = _rowType

  def family = _family

  def key = _key

  def superColumn = _rowType match {
    case Super => _superColumn
    case SuperCounter => _superColumn
    case _ => throw new InvalidRowTypeAccessException("superColumn does not apply to: " + _rowType)
  }

  def column = _column

  def value = _value

  def timestamp = _rowType match {
    case Standard => _timestamp
    case Super => _timestamp
    case _ => throw new InvalidRowTypeAccessException("timestamp does not apply to: " + _rowType)
  }

  def keyAs[T: Manifest]: T = as[T](key)

  def superColumnAs[T: Manifest]: T = as[T](superColumn)

  def columnAs[T: Manifest]: T = as[T](column)

  def valueAs[T: Manifest]: T = as[T](value)

  protected[this] def as[T: Manifest](b: ByteBuffer): T =
    Serializers.fromClassBytes(manifest[T].erasure, b).asInstanceOf[T]

  override def toString = new ToStringBuilder(this).append("rowType", rowType.name).toString
}
