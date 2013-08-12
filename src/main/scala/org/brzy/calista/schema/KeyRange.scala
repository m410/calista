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

import org.brzy.calista.serializer.Serializers._
import org.brzy.calista.Calista

/**
 * Used to query the datastore for multiple keys.
 * <p>
 * Note that this is not accessible via the dsl.
 *
 * @param start The first key to return.
 * @param finish The last key to return.
 * @param predicate Predicate to refine the query.
 * @param columnFamily The parent column family.
 * @param count The max number of result, defaults to 100.
 *
 * @author Michael Fortin
 */
class KeyRange[T] protected[schema](
        val start: Option[T] = None,
        val startBytes: Array[Byte] = Array.empty[Byte],
        val finish: Option[T] = None,
        val finishBytes: Array[Byte] = Array.empty[Byte],
        val predicate: Option[SlicePredicate[_]] = None,
        val columnFamily: Family,
        val count: Int = 100) {

  def columnParent = ColumnParent(columnFamily.name, null)

  def to[K](key: K) = {
    def keyBytes = toBytes(key).array()
    new KeyRange(start, startBytes, Option(key), keyBytes, predicate, columnFamily, count)
  }

  def from[K](key: K) = {
    def keyBytes = toBytes(key).array()
    new KeyRange(Option(key), keyBytes, finish, finishBytes, predicate, columnFamily, count)
  }

  def size(max: Int) = {
    new KeyRange(start, startBytes, finish, finishBytes, predicate, columnFamily, max)
  }

  def predicate[N](columnNames: Array[N]) = {
    val p = Option(new SlicePredicate(columnNames, null))
    new KeyRange(start, startBytes, finish, finishBytes, p, columnFamily, count)
  }


  def list = {
    Calista.value.keyRange(this)
  }
}