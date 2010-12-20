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

import org.brzy.calista.serializer.Serializers

/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
case class SuperKey[T](key:T,family:ColumnFamily)(implicit t:Manifest[T]) {
  def |[N](sKey:N)(implicit n:Manifest[N]) = SuperColumn(sKey,this)
  def keyBytes = {
    val buf = Serializers.toBytes(key)
    buf
  }
}