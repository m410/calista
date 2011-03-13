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
import org.brzy.calista.schema.{Column=>SColumn,StandardKey}
import org.brzy.calista.serializer.{UTF8Serializer, Serializer}

import org.slf4j.LoggerFactory
import collection.JavaConversions._

/**
 * Defines the object to column mapping.
 *
 * @tparam T The type of the object to map to the datastore.
 * @param columnSerializer What serializer to use to read and write the column names.
 * @param overrideFamily Sets the family name is its something other than the name of the class.
 * @param attributes The fields of the object, mapped to column values.
 *
 * @author Michael Fortin
 */
case class Mapping[T <: StandardEntity[_] : Manifest](
        family: String,
        columnNameSerializer: Serializer[_],
        attributes: Attribute*) {
  protected[this] val log = LoggerFactory.getLogger(classOf[Mapping[_]])

	/**
	 * Creates a new instance from the key and list of columns.
	 */
  def newInstance[K](key:K, columns: List[RColumn]): T = newInstance(key,null,columns)

  /**
	 * Creates a new instance from the key and list of columns.
	 */
  def newInstance[K,S](key:K, superColumn:S, columns: List[RColumn]): T = {
    val constructor = manifest[T].erasure.getConstructors()(0)
    val paramTypes = constructor.getParameterTypes
    var attrIdx = -1
    val args = attributes.map(attr=>{
      attrIdx = attrIdx + 1
      attr match {
        case k:Key => key
        case s:SuperColumn => superColumn
        case c:Column =>
          columns.find(_.nameAs(columnNameSerializer) == c.name) match {
            case Some(c) => c.valueAs(paramTypes(attrIdx))
            case _ => null // todo set a default value
          }
        case _=>
      }
    })
    val toArray = args.toArray.asInstanceOf[Array[java.lang.Object]]
    log.debug("args: {}",toArray.mkString("[",",","]"))
    constructor.newInstance(toArray:_*).asInstanceOf[T]
  }

	/**
	 * Creates a list of columns from the persistable object.
	 */
  def toColumns(t: T): List[SColumn[_, _]] = {
    import org.brzy.calista.schema.Conversions._
    val key = attributes.find(_.isInstanceOf[Key]).get
    val superColOption = attributes.find(_.isInstanceOf[SuperColumn])
    val clazz = t.getClass
    attributes.filter(a=>{!a.isInstanceOf[Key] && !a.isInstanceOf[SuperColumn]}).map(a=>{
      val col = a.asInstanceOf[Column]
      val colValue = clazz.getMethod(col.name).invoke(t)
      val k = clazz.getMethod("key").invoke(t)

      superColOption match {
        case Some(s) =>
          val suCol = clazz.getMethod("superColumn").invoke(t)
          family |^ k | suCol | (col.name, colValue)
        case _ =>
          family | k | (col.name, colValue)
      }
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

