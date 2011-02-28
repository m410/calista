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
 * Defines the object to column mapping.
 *
 * @tparam T The type of the object to map to the datastore.
 * @param overrideFamily Sets the family name is its something other than the name of the class.
 * @param columnSerializer What serializer to use to read and write the column names.
 * @param attributes The fields of the object, mapped to column values.
 *
 * @author Michael Fortin
 */
case class ColumnMapping[T <: KeyedEntity[_] : Manifest](
        overrideFamily: String = null,
        columnSerializer: Serializer[_] = null,
        attributes: Array[Attribute] = Array.empty[Attribute]) {
  protected[this] val log = LoggerFactory.getLogger(classOf[ColumnMapping[_]])

	/**
	 *	The name of the column family that the object maps too.
	 */
  val family = if (overrideFamily == null)
    manifest[T].erasure.getSimpleName
  else
    overrideFamily

	/**
	 *
	 */
  def attributes(serializer: Serializer[_], columns: Array[Attribute]): ColumnMapping[T] = {
    new ColumnMapping(overrideFamily = family, columnSerializer = serializer, attributes = columns)
  }

	/**
	 * Creates a new instance from the key and list of columns.
	 */
  def newInstance[K](key:K, columns: List[RColumn]): T = {
    val constructor = manifest[T].erasure.getConstructors()(0)
    val paramTypes = constructor.getParameterTypes

    val args = for(val i <- 0 until attributes.length) yield {
      val attr = attributes(i)

      if(attr.key)
        key
      else {
        columns.find(_.nameAs(columnSerializer) == attr.name) match {
          case Some(c) => c.valueAs(paramTypes(i))
          case _ => null // todo set a default value
        }
      }
    }
    val toArray = args.toArray.asInstanceOf[Array[java.lang.Object]]
    log.debug("args: {}",toArray.mkString("[",",","]"))
    constructor.newInstance(toArray:_*).asInstanceOf[T]
  }

	/**
	 * Creates a list of columns from the persistable object.
	 */
  def toColumns(t: T): List[Column[_, _]] = {
    import org.brzy.calista.schema.Conversions._
    val key = attributes.find(_.key).get
    val clazz = t.getClass
    attributes.filter(!_.key).map(a=>{
      family | clazz.getMethod(key.name).invoke(t) | (a.name, clazz.getMethod(a.name).invoke(t))
    }).toList
  }

	/**
	 *
	 */
  def toKey(t: T): StandardKey[_] = {
    import org.brzy.calista.schema.Conversions._
    family | t.key
  }
}

