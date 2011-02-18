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
 * Used to query the datastore for multiple keys.
 * 
 * @param start The first key to return.
 * @param finish The last key to return.
 * @param predicate Predicate to refine the query.
 * @param columnFamily The parent column family.
 * @param count The max number of result, defaults to 100.
 * 
 * @author Michael Fortin
 */
case class KeyRange[T,C](
        start:T,
        finish:T,
        predicate:SlicePredicate[C],
        columnFamily:ColumnFamily,
        count:Int = 100) {

  def startBytes =
    if(start != null)
      Serializers.toBytes(start)
    else
      null

  def finishBytes =
    if(finish != null)
      Serializers.toBytes(finish)
    else
      null

  def columnParent = ColumnParent(columnFamily.name, null)
}