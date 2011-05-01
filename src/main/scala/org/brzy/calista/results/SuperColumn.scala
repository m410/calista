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
import org.brzy.calista.schema.ColumnOrSuperColumn
import org.brzy.calista.serializer.{Serializers, Serializer}

/**
 * Represents the return value of a query that returns a super column
 *
 * @author Michael Fortin
 */
case class SuperColumn[T](bytes: Array[Byte], serializer: Serializer[T], columns: List[Column])
        extends ColumnOrSuperColumn {

  def key: T = serializer.fromBytes(bytes)

  def keyAs[T]()(implicit m:Manifest[T]) = Serializers.fromClassBytes(m.erasure,bytes)
}