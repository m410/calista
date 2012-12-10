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
import org.brzy.calista.serializer.UTF8Serializer

/**
 *
 * @author Michael Fortin
 */
sealed abstract class MappingAttribute {
  def name: String

  def serializer: Serializer[_]
}

/**
 * An attribute of a scala object that is mapped to a cassandra column.
 *
 * @param serializer How to read and write the datatype to the database.
 */
case class Key(name: String, serializer: Serializer[_] = UTF8Serializer) extends MappingAttribute

/**
 * An attribute of a scala object that is mapped to a cassandra column.
 *
 * @param serializer How to read and write the datatype to the database.
 */
case class SuperColumn(name: String, serializer: Serializer[_] = UTF8Serializer) extends MappingAttribute

/**
 * An attribute of a scala object that is mapped to a cassandra column.
 *
 * @param serializer How to read and write the datatype to the database.
 */
case class Column(name: String, serializer: Serializer[_] = UTF8Serializer) extends MappingAttribute


