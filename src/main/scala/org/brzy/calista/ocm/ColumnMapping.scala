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
package org.brzy.calista.ocm

import org.brzy.calista.results.{Column=>RColumn}
import org.brzy.calista.schema.Column
import org.brzy.calista.schema.StandardKey
import org.brzy.calista.serializer.Serializer
import collection.JavaConversions._
import org.slf4j.LoggerFactory

/**
 * @author Michael Fortin
 */
case class ColumnMapping[T <: KeyedEntity[_] : Manifest](
        overrideFamily: String = null,
        columnSerializer: Serializer[_] = null,
        attributes: Array[Attribute] = Array.empty[Attribute]) {
  val log = LoggerFactory.getLogger(classOf[ColumnMapping[_]])

  val family = if (overrideFamily == null)
    manifest[T].erasure.getSimpleName
  else
    overrideFamily

  def attributes(serializer: Serializer[_], columns: Array[Attribute]): ColumnMapping[T] = {
    new ColumnMapping(overrideFamily = family, columnSerializer = serializer, attributes = columns)
  }

  def newInstance[K](key:K, columns: List[RColumn]): T = {
    val constructor = manifest[T].erasure.getConstructors()(0)
    val paramTypes = constructor.getParameterTypes

    val args = for(val i <- 0 until attributes.length) yield {
      val attr = attributes(i)

      if(attr.key)
        key
      else {
        val iCol = columns.find(_.nameAs(columnSerializer) == attr.name).get
        iCol.valueAs(paramTypes(i))
      }
    }
    val toArray = args.toArray.asInstanceOf[Array[java.lang.Object]]
    log.debug("args: {}",toArray.mkString("[",",","]"))
    constructor.newInstance(toArray:_*).asInstanceOf[T]
  }

  def toColumns(t: T): List[Column[_, _]] = {
    import org.brzy.calista.schema.Conversions._
    val key = attributes.find(_.key).get
    val clazz = t.getClass
    attributes.filter(!_.key).map(a=>{
      family | clazz.getMethod(key.name).invoke(t) | (a.name, clazz.getMethod(a.name).invoke(t))
    }).toList
  }

  def toKey(t: T): StandardKey[_] = {
    import org.brzy.calista.schema.Conversions._
    family | t.key
  }
}

