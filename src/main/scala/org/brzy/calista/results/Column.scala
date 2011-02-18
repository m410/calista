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
import java.util.Date
import org.brzy.calista.schema.ColumnOrSuperColumn
import org.brzy.calista.serializer.{Serializers, UTF8Serializer, Serializer}

/**
 * A column returned by cassandra.
 *
 * @author Michael Fortin
 */
case class Column(name: Array[Byte], value: Array[Byte], timestamp: Date) extends ColumnOrSuperColumn {
	
	/**
	 * Converts the name column byte array to a scala data type.
	 */
  def nameAs[T](s:Serializer[T]):T = s.fromBytes(name)

	/**
	 * Converts the value byte array of the column to a scala data type.
	 */
  def valueAs[T](s:Serializer[T]):T = s.fromBytes(value)

	/**
	 * Converts the value byte array of the column to a scala data type.
	 */
  def valueAs[T](c:Class[T]):T = Serializers.fromClassBytes(c,value)
}