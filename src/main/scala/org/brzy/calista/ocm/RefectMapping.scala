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

import org.brzy.calista.serializer.{UTF8Serializer,  Serializer}
import scala.reflect.runtime.universe.TypeTag


/**
 * Document Me..
 * 
 * @author Michael Fortin
 */
@deprecated("use reflection mapping","0.7.0")
class ReflectMapping[T<:AnyRef:Manifest,K] protected[ocm] (
        val columnFamilyName:String,
        val columnNameSerializer: Serializer[_] = UTF8Serializer,
        val superColumnKey: Option[MappingAttribute] = None,
        val primaryKey: Option[MappingAttribute] = None,
        val attributes: Seq[MappingAttribute] = Seq.empty[MappingAttribute])
  extends StandardMapping[T,K] {

  def columnNameSerializer(s:Serializer[_]) = {
    new ReflectMapping[T,K](
      columnFamilyName = columnFamilyName,
      columnNameSerializer = s,
      superColumnKey = superColumnKey,
      primaryKey = primaryKey,
      attributes = attributes)
  }

  def field(s:MappingAttribute) = {
    new ReflectMapping[T,K](
      columnFamilyName = columnFamilyName,
      columnNameSerializer = columnNameSerializer,
      superColumnKey = superColumnKey,
      primaryKey = primaryKey,
      attributes = attributes ++ Seq(s))
  }

  def key(s:MappingAttribute) = {
    new ReflectMapping[T,K](
      columnFamilyName = columnFamilyName,
      columnNameSerializer = columnNameSerializer,
      superColumnKey = superColumnKey,
      primaryKey = Option(s),
      attributes = attributes)
  }

  def superColumn(s:MappingAttribute) = {
    new ReflectMapping[T,K](
      columnFamilyName = columnFamilyName,
      columnNameSerializer = columnNameSerializer,
      superColumnKey = Option(s),
      primaryKey = primaryKey,
      attributes = attributes)
  }

  def newInstance(key: K) = {

    null.asInstanceOf[T]
  }

  def toColumns(instance: T)(implicit t:TypeTag[T]) = null

  def family = columnFamilyName

  def keyFor(t: T) = null.asInstanceOf[K]
}

@deprecated("use reflection mapping","0.7.0")
object ReflectMapping {
  def apply[T<:AnyRef:Manifest,K](columnFamilyName:String) = {
    new ReflectMapping[T,K]( columnFamilyName)
  }
}
