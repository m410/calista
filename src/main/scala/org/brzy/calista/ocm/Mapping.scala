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

import org.brzy.calista.serializer.Serializer
import org.brzy.calista.results.ResultSet

import org.scalastuff.scalabeans.Preamble._
import org.slf4j.LoggerFactory
import java.util.Date
import org.brzy.calista.schema.{SuperKey, ColumnFamily, StandardKey, Column => SColumn}

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
case class Mapping[T <: AnyRef : Manifest](
        family: String,
        columnNameSerializer: Serializer[_],
        attributes: MappingAttribute*) {
  protected[this] val log = LoggerFactory.getLogger(classOf[Mapping[_]])

  /**
	 * Creates a new instance from the key and list of columns.
	 */
  def newInstance[K:Manifest,S:Manifest](key:K, superColumn:Option[S], columns: ResultSet): T = {
    val colMap = columns.rows.map(r=>{
      columnNameSerializer.fromBytes(r.column) -> r
    }).toMap
    val descriptor = descriptorOf[T]
    val builder = descriptor.newBuilder()
    val attributeKey = attributes.find(_.isInstanceOf[Key]).get
    val keyProperty = descriptor.properties.find(_.name == attributeKey.name).get
    log.debug("key:"+keyProperty+":"+key)
    builder.set(keyProperty, key)

    superColumn match {
      case Some(sc) =>
        val attributeSuCol = attributes.find(_.isInstanceOf[SuperColumn]).get
        val scProperty = descriptor.properties.find(_.name == attributeSuCol.name).get
        log.debug("super:"+scProperty+":"+sc)
        builder.set(scProperty, sc)
      case _ =>
    }

    attributes.filter(_.isInstanceOf[Column]).foreach(column =>{
      val prop = descriptor.properties.find(_.name == column.name).get
      val value = column.serializer.fromBytes(colMap(column.name).value)
      log.debug("column:"+prop+":"+value)
      builder.set(prop, value)
    })

    builder.result().asInstanceOf[T]
  }

	/**
	 * Creates a list of columns from the persistable object.
	 */
  def toColumns(t: T): List[SColumn[_, _]] = {
    val descriptor = descriptorOf[T]
    val key = attributes.find(_.isInstanceOf[Key]).get
    val keyValue = descriptor.get(t,key.name)
    val superColOption = attributes.find(_.isInstanceOf[SuperColumn])

    attributes.filter(_.isInstanceOf[Column]).map(attr=>{
      val columnValue = descriptor.get(t,attr.name)

      superColOption match {
        case Some(s) =>
          val v = descriptor.get(t,s.name)
          ColumnFamily(family).superKey(keyValue).superColumn(v).column(attr.name,columnValue,new Date())
        case _ =>
          ColumnFamily(family).standardKey(keyValue).column(attr.name,columnValue,new Date())
      }
    }).toList
  }

	/**
	 *
	 */
  def toKey(t: T): Either[SuperKey[_],StandardKey[_]] = {
    val key = attributes.find(_ match {
      case k:Key => true
      case _ => false
    }).get
    val descriptor = descriptorOf[T]
    val keyValue = descriptor.get(t,key.name)

    if (attributes.find(_.isInstanceOf[SuperColumn]).isDefined)
      Left(ColumnFamily(family).superKey(keyValue))
    else
      Right(ColumnFamily(family).standardKey(keyValue))
  }
}

